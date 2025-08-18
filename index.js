// index.js (ESM)
import dotenv from 'dotenv'; dotenv.config();

import fetch from 'node-fetch';
import bolt from '@slack/bolt';
const { App, ExpressReceiver } = bolt;
import express from 'express';
import crypto from 'crypto';
import PDFDocument from 'pdfkit';
import QRCode from 'qrcode';
import FormData from 'form-data';

/* ========= ENV / CONSTANTS ========= */
const PORT = process.env.PORT || 3000;
const SLACK_SIGNING_SECRET = process.env.SLACK_SIGNING_SECRET;
const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN;
const PIPEDRIVE_API_TOKEN = process.env.PIPEDRIVE_API_TOKEN;
const SIGNING_SECRET = process.env.WO_QR_SECRET; // HMAC for signed QR links
const DEFAULT_CHANNEL = process.env.DEFAULT_SLACK_CHANNEL_ID || 'C098H8GU355';
const BASE_URL = process.env.BASE_URL; // e.g. https://dispatcher-xxx.up.railway.app
const PD_WEBHOOK_KEY = process.env.PD_WEBHOOK_KEY; // guard for /pipedrive-task
const ALLOW_DEFAULT_FALLBACK = process.env.ALLOW_DEFAULT_FALLBACK !== 'false'; // set to 'false' to disable fallback
const FORCE_CHANNEL_ID = process.env.FORCE_CHANNEL_ID || null; // hard route for debugging
const PD_PRODUCTION_TEAM_FIELD_KEY = process.env.PD_PRODUCTION_TEAM_FIELD_KEY || ''; // Deal custom field key for Production Team

// Feature toggles (set to 'false' to turn off)
const ENABLE_PD_FILE_UPLOAD = process.env.ENABLE_PD_FILE_UPLOAD !== 'false';            // default: true
const ENABLE_PD_NOTE = process.env.ENABLE_PD_NOTE !== 'false';                          // default: true
const ENABLE_SLACK_PDF_UPLOAD = process.env.ENABLE_SLACK_PDF_UPLOAD !== 'false';        // default: true
const ENABLE_DELETE_ON_REASSIGN = process.env.ENABLE_DELETE_ON_REASSIGN !== 'false';    // default: true

if (!SLACK_SIGNING_SECRET) throw new Error('Missing SLACK_SIGNING_SECRET');
if (!SLACK_BOT_TOKEN) throw new Error('Missing SLACK_BOT_TOKEN');
if (!PIPEDRIVE_API_TOKEN) throw new Error('Missing PIPEDRIVE_API_TOKEN');
if (!SIGNING_SECRET) throw new Error('Missing WO_QR_SECRET');
if (!BASE_URL) throw new Error('Missing BASE_URL (your public Railway URL)');
if (!PD_WEBHOOK_KEY) console.warn('⚠️ PD_WEBHOOK_KEY not set; /pipedrive-task will return 403.');

// ---- Global crash handlers ----
process.on('unhandledRejection', (reason) => {
  console.error('[FATAL] Unhandled Promise Rejection:', reason?.stack || reason);
});
process.on('uncaughtException', (err) => {
  console.error('[FATAL] Uncaught Exception:', err?.stack || err);
});

/* ========= Label maps ========= */
const SERVICE_MAP = {
  27: 'Water Mitigation',
  28: 'Fire Cleanup',
  29: 'Contents',
  30: 'Biohazard',
  31: 'General Cleaning',
  32: 'Duct Cleaning',
};

const PRODUCTION_TEAM_MAP = {
  47: 'Kings',
  48: 'Johnathan',
  49: 'Pena',
  50: 'Hector',
  51: 'Sebastian',
  52: 'Anastacio',
  53: 'Mike',
  54: 'Kim',
};

// Assignee Channel Overrides (Production Team enum ID → Slack channel ID)
const PRODUCTION_TEAM_TO_CHANNEL = {
  // Anastacio:
  52: 'C09BA0XUAV7',
  // Add others as you create their channels:
  // 53: 'Cxxxxxxxxxx', // Mike
  // 50: 'Cyyyyyyyyyy', // Hector
};

/* ========= Slack App (Bolt) ========= */
const receiver = new ExpressReceiver({
  signingSecret: SLACK_SIGNING_SECRET,
  endpoints: { events: '/slack/events', commands: '/slack/commands', actions: '/slack/interact' },
  processBeforeResponse: true,
});

const app = new App({ token: SLACK_BOT_TOKEN, signingSecret: SLACK_SIGNING_SECRET, receiver });

/* ========= Mentions ping ========= */
app.event('app_mention', async ({ event, say }) => {
  await say(`Hey <@${event.user}>, Dispatcher is online and running!`);
});

/* ========= Slack action: complete task checkbox ========= */
app.action('complete_task', async ({ body, ack, client }) => {
  await ack();
  const checkboxValue = body.actions?.[0]?.selected_options?.[0]?.value;
  const activityId = checkboxValue?.replace('task_', '');
  if (!activityId) return;

  try {
    const pdUrl = `https://api.pipedrive.com/v1/activities/${activityId}?api_token=${PIPEDRIVE_API_TOKEN}`;
    await fetch(pdUrl, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ done: true, marked_as_done_time: new Date().toISOString() }),
    });
  } catch (e) {
    console.error('PD complete error:', e);
  }

  try {
    await client.chat.delete({ channel: body.channel.id, ts: body.message.ts });
  } catch (e) {
    console.error('Slack delete error:', e);
  }
});

/* ========= Express app from Bolt receiver ========= */
const expressApp = receiver.app;
expressApp.use(express.json());
expressApp.get('/', (_req, res) => res.status(200).send('Dispatcher OK'));
expressApp.get('/healthz', (_req, res) => res.status(200).send('ok'));

/* ========= HMAC helpers for signed QR links ========= */
const b64url = (buf) => Buffer.from(buf).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
const sign = (raw) => b64url(crypto.createHmac('sha256', SIGNING_SECRET).update(raw).digest());

function verify(raw, sig) {
  try {
    const a = Buffer.from(sign(raw));
    const b = Buffer.from(String(sig));
    if (a.length !== b.length) return false;
    return crypto.timingSafeEqual(a, b);
  } catch {
    return false;
  }
}

/* ========= Signed URL builder for auto-complete ========= */
function makeSignedCompleteUrl({ aid, did = '', cid = '', ttlSeconds = 7 * 24 * 60 * 60 }) {
  const exp = Math.floor(Date.now() / 1000) + ttlSeconds;
  const raw = `${aid}.${did}.${cid}.${exp}`;
  const sig = sign(raw);
  const params = new URLSearchParams({ aid, exp: String(exp), sig });
  if (did) params.set('did', String(did));
  if (cid) params.set('cid', String(cid));
  return `${BASE_URL}/wo/complete?${params.toString()}`;
}

/* ========= Deal channel resolution ========= */
const channelCache = new Map(); // key: dealId -> channelId

async function findDealChannelId(dealId) {
  if (!dealId) return null;
  const key = String(dealId);
  if (channelCache.has(key)) return channelCache.get(key);

  const tokens = [
    `deal${key}`, `deal-${key}`, `deal_${key}`,
    `${key}`, `${key}-deal`, `${key}_deal`,
    `job${key}`, `job-${key}`, `job_${key}`,
  ];

  console.log(`[WO] searching Slack channels for deal ${key}; tokens: ${tokens.join(', ')}`);

  let cursor;
  const seen = new Set();
  while (true) {
    const resp = await app.client.conversations.list({
      types: 'public_channel,private_channel',
      limit: 1000,
      cursor,
      exclude_archived: true,
    });
    const chans = resp.channels || [];
    for (const c of chans) {
      if (!c?.id || !c?.name) continue;
      if (seen.has(c.id)) continue;
      seen.add(c.id);

      const exact = tokens.includes(c.name);
      const suffix = tokens.some(t => c.name.endsWith(t));
      const strip = s => String(s).toLowerCase().replace(/[^a-z0-9]/g, '');
      const fuzzy = strip(c.name).includes(strip(`deal${key}`)); // e.g., "tammiehalldeal135"

      if (exact || suffix || fuzzy) {
        console.log(`[WO] matched channel "${c.name}" (${c.id}) for deal ${key}`);
        channelCache.set(key, c.id);
        return c.id;
      }
    }
    if (!resp.response_metadata?.next_cursor) break;
    cursor = resp.response_metadata.next_cursor;
  }

  console.warn(`[WO] no channel match for deal ${key}`);
  return null;
}

async function ensureBotInChannel(channelId) {
  if (!channelId) return;
  try {
    await app.client.conversations.join({ channel: channelId });
  } catch (e) {
    const code = e?.data?.error || e?.message;
    if (code && !['method_not_supported_for_channel_type', 'is_archived', 'already_in_channel', 'not_in_channel'].includes(code)) {
      console.log('[WO] join note:', code);
    }
  }
}

async function resolveDealChannelId({ dealId, allowDefault = ALLOW_DEFAULT_FALLBACK }) {
  const byDeal = await findDealChannelId(dealId);
  if (byDeal) return byDeal;
  return allowDefault ? DEFAULT_CHANNEL : null;
}

/* ========= Assignee channel resolution (Production Team → channel) ========= */
function resolveAssigneeInfoFromDeal(deal) {
  if (!deal || !PD_PRODUCTION_TEAM_FIELD_KEY) return { teamId: null, teamName: null, channelId: null };
  const teamId = deal[PD_PRODUCTION_TEAM_FIELD_KEY] || null; // enum ID number
  const teamName = teamId ? PRODUCTION_TEAM_MAP[teamId] || `Team ${teamId}` : null;
  const channelId = teamId ? PRODUCTION_TEAM_TO_CHANNEL[teamId] || null : null;
  return { teamId, teamName, channelId };
}

/* ========= In-memory registry to manage reassignment deletes ========= */
// We delete only in the *assignee* channel (job channel is permanent history).
// Map: activityId -> { assigneeChannelId, messageTs }
const ASSIGNEE_POSTS = new Map();

/* ========= Build Work Order PDF (returns Buffer) ========= */
async function buildWorkOrderPdfBuffer({ activity, dealTitle, typeOfService, location, channelForQR, assigneeName }) {
  const completeUrl = makeSignedCompleteUrl({
    aid: String(activity.id),
    did: activity.deal_id ? String(activity.deal_id) : '',
    cid: channelForQR || DEFAULT_CHANNEL,
  });

  const qrDataUrl = await QRCode.toDataURL(completeUrl);
  const qrBase64 = qrDataUrl.replace(/^data:image\/png;base64,/, '');
  const qrBuffer = Buffer.from(qrBase64, 'base64');

  const doc = new PDFDocument({ margin: 36 });
  const chunks = [];
  doc.on('data', (c) => chunks.push(c));
  const done = new Promise((resolve) => doc.on('end', () => resolve(Buffer.concat(chunks))));

  doc.fontSize(20).text('Work Order', { align: 'center' });
  doc.moveDown(0.25);
  doc.fontSize(10).fillColor('#666').text(new Date().toLocaleString(), { align: 'center' });
  doc.moveDown(1);

  const scheduled = [activity.due_date, activity.due_time].filter(Boolean).join(' ').trim();

  doc.fillColor('#000').fontSize(12);
  doc.text(`Task: ${activity.subject || '-'}`);
  doc.text(`Due:  ${scheduled || '-'}`);
  doc.text(`Deal: ${dealTitle || '-'}`);
  doc.text(`Type of Service: ${typeOfService || '-'}`);
  doc.text(`Location: ${location || '-'}`);
  if (assigneeName) doc.text(`Assigned To: ${assigneeName}`);
  doc.moveDown(0.5);

  const rawNote = (activity.note || '')
    .replace(/<br\/?>(\s)?/g, '\n')
    .replace(/&nbsp;/g, ' ')
    .trim();

  if (rawNote) {
    doc.font('Helvetica-Bold').text('Scope / Notes');
    doc.font('Helvetica').text(rawNote, { width: 520 });
    doc.moveDown(0.5);
  }

  doc.font('Helvetica-Bold').text('Scan to Complete');
  doc.font('Helvetica').fontSize(10).fillColor('#555').text('Scanning marks this task complete in Pipedrive and posts a confirmation in Slack.');
  doc.moveDown(0.5);
  doc.image(qrBuffer, { fit: [120, 120] }); // smaller QR
  doc.moveDown(0.25);
  doc.fontSize(8).fillColor('#777').text(completeUrl, { width: 520 });

  doc.end();
  return done;
}

/* ========= Upload PDF to Slack (files.uploadV2) ========= */
async function uploadPdfToSlack({ channel, filename, pdfBuffer, title, initialComment = '' }) {
  const result = await app.client.files.uploadV2({
    channel_id: channel,
    file: pdfBuffer,
    filename,
    title: title || filename,
    initial_comment: initialComment,
  });
  return result;
}

/* ========= Upload PDF to Pipedrive Deal Files ========= */
async function uploadPdfToPipedrive({ dealId, pdfBuffer, filename }) {
  if (!dealId || dealId === 'N/A') return;
  const form = new FormData();
  form.append('deal_id', String(dealId));
  form.append('file', pdfBuffer, { filename: filename || 'workorder.pdf', contentType: 'application/pdf' });
  const resp = await fetch(
    `https://api.pipedrive.com/v1/files?api_token=${PIPEDRIVE_API_TOKEN}`,
    { method: 'POST', headers: form.getHeaders(), body: form }
  );
  const j = await resp.json();
  if (!j?.success) {
    console.error('Pipedrive file upload failed:', j);
    throw new Error('PD file upload failed');
  }
  return j;
}

/* ========= Pipedrive webhook → Slack + PDF + PD file (SAFE WRAPPER) ========= */
expressApp.post('/pipedrive-task', async (req, res) => {
  const startedAt = Date.now();

  try {
    if (!PD_WEBHOOK_KEY || req.query.key !== PD_WEBHOOK_KEY) {
      console.warn('[PD Hook] Forbidden: missing/invalid key', { hasEnv: !!PD_WEBHOOK_KEY, got: req.query.key });
      return res.status(403).send('Forbidden');
    }

    const hdrs = {
      'content-type': req.headers['content-type'],
      'user-agent': req.headers['user-agent'],
      'x-forwarded-for': req.headers['x-forwarded-for'],
    };
    const bodyPreview = JSON.stringify(req.body).slice(0, 5000);
    console.log('[PD Hook] Incoming', { headers: hdrs, query: req.query, bodyPreview });

    const action = req.body?.meta?.action || 'unknown';
    const activity = req.body?.data || req.body?.current || null;
    if (!activity?.id) {
      console.warn('[PD Hook] No activity in payload; returning 200 to avoid PD retries.');
      return res.status(200).send('No activity.');
    }

    // Fetch full activity
    let aJson;
    try {
      const aRes = await fetch(
        `https://api.pipedrive.com/v1/activities/${encodeURIComponent(activity.id)}?api_token=${PIPEDRIVE_API_TOKEN}`
      );
      aJson = await aRes.json();
    } catch (e) {
      console.error('[PD Hook] Fetch activity failed:', e?.stack || e);
      throw new Error('Failed to fetch activity from Pipedrive');
    }
    if (!aJson?.success || !aJson.data) {
      console.error('[PD Hook] Activity fetch not successful:', aJson);
      throw new Error('Activity fetch returned no data');
    }

    // Pull details and deal
    let fullNote = '_No note provided_';
    let dealId = 'N/A';
    let dealTitle = 'N/A';
    let typeOfService = 'N/A';
    let location = 'N/A';
    let productionTeamId = null;
    let productionTeamName = null;
    let assigneeChannelId = null;

    const data = aJson.data;
    fullNote = (data.note || fullNote).replace(/<br\/?>(\s)?/g, '\n').replace(/&nbsp;/g, ' ').trim();
    dealId = data.deal_id || 'N/A';

    if (dealId && dealId !== 'N/A') {
      try {
        const dRes = await fetch(
          `https://api.pipedrive.com/v1/deals/${encodeURIComponent(dealId)}?api_token=${PIPEDRIVE_API_TOKEN}`
        );
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) {
          const deal = dJson.data;
          dealTitle = deal.title || 'N/A';
          const serviceId = deal['5b436b45b63857305f9691910b6567351b5517bc'];
          typeOfService = SERVICE_MAP[serviceId] || serviceId || 'N/A';
          location = deal.location || 'N/A';

          const assigneeInfo = resolveAssigneeInfoFromDeal(deal);
          productionTeamId = assigneeInfo.teamId;
          productionTeamName = assigneeInfo.teamName;
          assigneeChannelId = assigneeInfo.channelId;
        } else {
          console.warn('[PD Hook] Deal fetch not successful', dJson);
        }
      } catch (e) {
        console.error('[PD Hook] Fetch deal failed:', e?.stack || e);
      }
    }

    // Resolve job channel, apply FORCE_CHANNEL_ID if set (for debugging)
    let dealChannelId = await resolveDealChannelId({ dealId, allowDefault: ALLOW_DEFAULT_FALLBACK });
    if (FORCE_CHANNEL_ID) {
      console.warn('[WO] FORCE_CHANNEL_ID set — overriding deal channel to', FORCE_CHANNEL_ID);
      dealChannelId = FORCE_CHANNEL_ID;
    }
    console.log('[WO] using Deal channel', dealChannelId, 'for deal', dealId, 'action:', action);

    // Build PDF (with assignee name on doc)
    let pdfBuffer;
    try {
      pdfBuffer = await buildWorkOrderPdfBuffer({
        activity: data,
        dealTitle,
        typeOfService,
        location,
        channelForQR: dealChannelId || DEFAULT_CHANNEL,
        assigneeName: productionTeamName,
      });
    } catch (e) {
      console.error('[WO] buildWorkOrderPdfBuffer failed:', e?.stack || e);
      throw new Error('Failed to build PDF');
    }

    const safe = (s) => (s || '').toString().replace(/[^\w\-]+/g, '_').slice(0, 60);
    const filename = `WO_${safe(dealTitle)}_${safe(activity.subject)}.pdf`;
    const scheduledText = [activity.due_date, activity.due_time].filter(Boolean).join(' ') || 'No due date';

    // Slack blocks shared
    const baseBlocks = [
      {
        type: 'section',
        text: {
          type: 'mrkdwn',
          text:
            `📌 *New Task*\n• *${activity.subject || '-'}*` +
            `\n🗕️ Due: ${scheduledText}` +
            `\n📜 Note: ${fullNote}` +
            `\n🏷️ Deal ID: ${dealId} - *${dealTitle}*` +
            `\n📦 Type of Service: ${typeOfService}` +
            `\n📍 Location: ${location}` +
            (productionTeamName ? `\n👷 Assigned To: ${productionTeamName}` : ''),
        },
      },
      {
        type: 'actions',
        elements: [
          {
            type: 'checkboxes',
            action_id: 'complete_task',
            options: [{ text: { type: 'mrkdwn', text: 'Mark as complete' }, value: `task_${activity.id}` }],
          },
          { type: 'button', text: { type: 'plain_text', text: 'Open Work Order PDF' }, url: `${BASE_URL}/wo/pdf?aid=${encodeURIComponent(activity.id)}` },
        ],
      },
    ];

    // 1) Deal (job) channel — persistent
    if (dealChannelId) {
      await ensureBotInChannel(dealChannelId);
      let dealMsg;
      try {
        dealMsg = await app.client.chat.postMessage({
          channel: dealChannelId,
          text: `📌 New Task • ${activity.subject || '-'} • Due: ${scheduledText}`,
          blocks: baseBlocks,
        });
        console.log('[WO] deal task card posted in', dealChannelId);
      } catch (e) {
        console.error('[WO] chat.postMessage (deal) failed:', e?.data || e?.message || e);
        throw new Error('Slack post (deal) failed');
      }

      if (ENABLE_SLACK_PDF_UPLOAD) {
        try {
          await uploadPdfToSlack({
            channel: dealChannelId,
            filename,
            pdfBuffer,
            title: `Work Order — ${activity.subject || ''}`,
            initialComment: '📄 Work Order PDF (scan QR to complete)',
          });
          console.log('[WO] Slack PDF uploaded to deal channel');
        } catch (e) {
          console.error('[WO] files.uploadV2 (deal) failed:', e?.data || e?.message || e);
          throw new Error('Slack file upload (deal) failed');
        }
      } else {
        console.log('[WO] Slack PDF upload disabled by env (deal channel)');
      }
    } else {
      console.warn('[WO] No Slack deal channel found; skipping job-channel post for deal', dealId);
    }

    // 2) Assignee channel — delete on reassign
    if (assigneeChannelId) {
      await ensureBotInChannel(assigneeChannelId);

      const prev = ASSIGNEE_POSTS.get(String(activity.id));
      if (prev?.assigneeChannelId && prev.assigneeChannelId !== assigneeChannelId && ENABLE_DELETE_ON_REASSIGN) {
        try {
          await app.client.chat.delete({ channel: prev.assigneeChannelId, ts: prev.messageTs });
          console.log('[WO] deleted previous assignee message from', prev.assigneeChannelId, '(reassigned)');
        } catch (e) {
          console.warn('[WO] failed to delete previous assignee message (ok):', e?.data || e?.message || e);
        }
      }

      let assigneeMsg;
      try {
        assigneeMsg = await app.client.chat.postMessage({
          channel: assigneeChannelId,
          text: `📌 New Task • ${activity.subject || '-'} • Due: ${scheduledText}`,
          blocks: baseBlocks,
        });
        console.log('[WO] assignee task card posted in', assigneeChannelId);

        ASSIGNEE_POSTS.set(String(activity.id), { assigneeChannelId, messageTs: assigneeMsg.ts });
      } catch (e) {
        console.error('[WO] chat.postMessage (assignee) failed:', e?.data || e?.message || e);
      }

      if (ENABLE_SLACK_PDF_UPLOAD) {
        try {
          await uploadPdfToSlack({
            channel: assigneeChannelId,
            filename,
            pdfBuffer,
            title: `Work Order — ${activity.subject || ''}`,
            initialComment: '📄 Work Order PDF (scan QR to complete)',
          });
          console.log('[WO] Slack PDF uploaded to assignee channel');
        } catch (e) {
          console.error('[WO] files.uploadV2 (assignee) failed:', e?.data || e?.message || e);
        }
      } else {
        console.log('[WO] Slack PDF upload disabled by env (assignee channel)');
      }
    } else {
      console.log('[WO] no assignee channel (missing PD_PRODUCTION_TEAM_FIELD_KEY or mapping). Skipping assignee channel.');
    }

    // 3) Pipedrive attachments / notes (toggleable)
    if (ENABLE_PD_FILE_UPLOAD) {
      try {
        await uploadPdfToPipedrive({ dealId, pdfBuffer, filename });
        console.log('[WO] PD file upload done');
      } catch (e) {
        console.error('[WO] PD file upload failed:', e);
      }
    } else {
      console.log('[WO] PD file upload disabled by env');
    }

    if (ENABLE_PD_NOTE) {
      const completeUrl = makeSignedCompleteUrl({
        aid: String(activity.id),
        did: dealId !== 'N/A' ? String(dealId) : '',
        cid: dealChannelId || DEFAULT_CHANNEL,
      });
      await fetch(`https://api.pipedrive.com/v1/notes?api_token=${PIPEDRIVE_API_TOKEN}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ deal_id: dealId, content: `Work Order posted to Slack.\nScan to complete: ${completeUrl}` }),
      });
      console.log('[WO] PD note posted');
    } else {
      console.log('[WO] PD note disabled by env');
    }

    console.log('[PD Hook] OK in', Date.now() - startedAt, 'ms');
    return res.status(200).send('OK');
  } catch (error) {
    console.error('[PD Hook] ERROR:', error?.stack || error?.data || error?.message || error);
    return res.status(500).send('Server error.');
  }
});

/* ========= QR scan → complete task in Pipedrive (+ Slack ping to job channel) ========= */
expressApp.get('/wo/complete', async (req, res) => {
  try {
    const { aid, did, cid, exp, sig } = req.query || {};
    if (!aid || !exp || !sig) return res.status(400).send('Missing params.');
    const now = Math.floor(Date.now() / 1000);
    if (Number(exp) < now) return res.status(410).send('Link expired.');
    const raw = `${aid}.${did || ''}.${cid || ''}.${exp}`;
    if (!verify(raw, sig)) return res.status(403).send('Bad signature.');

    const pdUrl = `https://api.pipedrive.com/v1/activities/${encodeURIComponent(aid)}?api_token=${PIPEDRIVE_API_TOKEN}`;
    const pdPayload = { done: true, marked_as_done_time: new Date().toISOString() };
    const pdResp = await fetch(pdUrl, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(pdPayload),
    });
    const pdJson = await pdResp.json();
    const ok = pdJson && pdJson.success;

    const channel = cid || DEFAULT_CHANNEL; // confirm in job channel
    if (channel) {
      const text = ok
        ? `✅ Task *${aid}* marked complete in Pipedrive${did ? ` (deal ${did})` : ''}.`
        : `⚠️ Tried to complete task *${aid}* but Pipedrive didn’t confirm success.`;
      await app.client.chat.postMessage({ channel, text });
    }

    res
      .status(200)
      .send(
        `<html><body style="font-family:Arial;padding:24px"><h2>Work Order Complete</h2><p>Task <b>${aid}</b> ${
          ok ? 'has been updated' : 'could not be updated'
        } in Pipedrive.</p>${did ? `<p>Deal: <b>${did}</b></p>` : ''}<p>${
          ok ? 'You’re good to go. ✅' : 'Please contact the office. ⚠️'
        }</p></body></html>`
      );
  } catch (err) {
    console.error('/wo/complete error:', err);
    res.status(500).send('Server error.');
  }
});

/* ========= Debug routes ========= */
expressApp.get('/debug/pdf', async (_req, res) => {
  try {
    const doc = new PDFDocument({ margin: 36 });
    const chunks = [];
    doc.on('data', (c) => chunks.push(c));
    doc.on('end', () => {
      const buf = Buffer.concat(chunks);
      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Content-Length', buf.length);
      res.send(buf);
    });
    doc.fontSize(20).text('Dispatcher PDF Test', { align: 'center' });
    doc.moveDown();
    doc.fontSize(12).text('If you can read this, pdfkit is working on Railway.');
    doc.end();
  } catch (e) {
    console.error('DEBUG /pdf error:', e);
    res.status(500).send('error');
  }
});

expressApp.get('/debug/upload-test', async (req, res) => {
  try {
    const channel = req.query.cid || DEFAULT_CHANNEL;
    const doc = new PDFDocument({ margin: 36 });
    const chunks = [];
    doc.on('data', (c) => chunks.push(c));
    const finished = new Promise((r) => doc.on('end', () => r(Buffer.concat(chunks))));
    doc.fontSize(18).text('Dispatcher Slack Upload Test', { align: 'center' });
    doc.moveDown().fontSize(12).text(`Channel: ${channel}`);
    doc.end();
    const pdfBuffer = await finished;

    const result = await app.client.files.uploadV2({
      channel_id: channel,
      file: pdfBuffer,
      filename: 'dispatcher-test.pdf',
      title: 'Dispatcher Test PDF',
      initial_comment: 'Test upload from /debug/upload-test',
    });
    console.log('DEBUG uploadV2 result:', result.ok);
    res.status(200).send('Uploaded test PDF to Slack with files.uploadV2.');
  } catch (e) {
    console.error('DEBUG /upload-test error:', e?.data || e);
    res.status(500).send(`upload failed: ${e?.data ? JSON.stringify(e?.data) : e.message}`);
  }
});

/* ========= On-demand Work Order PDF ========= */
expressApp.get('/wo/pdf', async (req, res) => {
  try {
    const aid = req.query.aid;
    if (!aid) return res.status(400).send('Missing aid');

    const aRes = await fetch(
      `https://api.pipedrive.com/v1/activities/${encodeURIComponent(aid)}?api_token=${PIPEDRIVE_API_TOKEN}`
    );
    const { success, data } = await aRes.json();
    if (!success || !data) return res.status(404).send('Activity not found');

    let dealTitle = 'N/A', typeOfService = 'N/A', location = 'N/A', assigneeName = null;
    const dealId = data.deal_id;
    if (dealId) {
      const dRes = await fetch(
        `https://api.pipedrive.com/v1/deals/${encodeURIComponent(dealId)}?api_token=${PIPEDRIVE_API_TOKEN}`
      );
      const dj = await dRes.json();
      if (dj?.success && dj.data) {
        dealTitle = dj.data.title || 'N/A';
        const serviceId = dj.data['5b436b45b63857305f9691910b6567351b5517bc'];
        typeOfService = SERVICE_MAP[serviceId] || serviceId || 'N/A';
        location = dj.data.location || 'N/A';
        if (PD_PRODUCTION_TEAM_FIELD_KEY && dj.data[PD_PRODUCTION_TEAM_FIELD_KEY]) {
          const tid = dj.data[PD_PRODUCTION_TEAM_FIELD_KEY];
          assigneeName = PRODUCTION_TEAM_MAP[tid] || `Team ${tid}`;
        }
      }
    }

    const channelIdForQr = await resolveDealChannelId({ dealId, allowDefault: ALLOW_DEFAULT_FALLBACK });
    const pdfBuffer = await buildWorkOrderPdfBuffer({
      activity: data,
      dealTitle,
      typeOfService,
      location,
      channelForQR: channelIdForQr || DEFAULT_CHANNEL,
      assigneeName,
    });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="WO_${aid}.pdf"`);
    return res.send(pdfBuffer);
  } catch (e) {
    console.error('/wo/pdf error', e);
    res.status(500).send('error');
  }
});

/* ========= Start ========= */
receiver.app.listen(PORT, () => {
  console.log(`✅ Dispatcher running on port ${PORT}`);
});
