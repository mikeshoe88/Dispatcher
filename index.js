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

if (!SLACK_SIGNING_SECRET) throw new Error('Missing SLACK_SIGNING_SECRET');
if (!SLACK_BOT_TOKEN) throw new Error('Missing SLACK_BOT_TOKEN');
if (!PIPEDRIVE_API_TOKEN) throw new Error('Missing PIPEDRIVE_API_TOKEN');
if (!SIGNING_SECRET) throw new Error('Missing WO_QR_SECRET');
if (!BASE_URL) throw new Error('Missing BASE_URL (your public Railway URL)');
if (!PD_WEBHOOK_KEY) console.warn('⚠️ PD_WEBHOOK_KEY not set. /pipedrive-task will reject requests.');

/* ========= Label maps ========= */
const SERVICE_MAP = {
  27: 'Water Mitigation',
  28: 'Fire Cleanup',
  29: 'Contents',
  30: 'Biohazard',
  31: 'General Cleaning',
  32: 'Duct Cleaning',
};

// Optional: Pipedrive production team enum ID -> Name (for reference)
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

// Slack user IDs (DM targets) keyed by Name above or by PD enum id.
// Fill out as you confirm IDs. For testing we use Mike per your instruction.
const SLACK_USER_IDS = {
  Mike: 'U05FPCPHJG6',
  Kim: 'U05FYG3EMHS',
  Hector: 'U05TUQ48UBU',
  Sebastian: 'U07827VLWNL', // if duplicate exists, keep the correct one
  Anastacio: 'U07AB7A4UNS',
  // Add Johnathan / Pena when you have them
};

/* ========= Slack App (Bolt) ========= */
const receiver = new ExpressReceiver({
  signingSecret: SLACK_SIGNING_SECRET,
  endpoints: {
    events: '/slack/events',
    commands: '/slack/commands',
    actions: '/slack/interact',
  },
  processBeforeResponse: true,
});

const app = new App({
  token: SLACK_BOT_TOKEN,
  signingSecret: SLACK_SIGNING_SECRET,
  receiver,
});

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
const b64url = (buf) =>
  Buffer.from(buf).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

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

/* ========= Resolve Slack channel for a deal ========= */
/** Cache channel lookups to limit API calls */
const channelCache = new Map(); // key: dealId -> channelId

/** Try channel names like "deal123", "deal-123" */
async function findDealChannelId(dealId) {
  if (!dealId) return null;
  if (channelCache.has(dealId)) return channelCache.get(dealId);

  const candidates = [`deal${dealId}`, `deal-${dealId}`];

  // conversations.list pagination
  let cursor;
  const seen = new Set();
  while (true) {
    const resp = await app.client.conversations.list({
      types: 'public_channel,private_channel',
      limit: 1000,
      cursor,
    });
    const chans = resp.channels || [];
    for (const c of chans) {
      if (!c || !c.name) continue;
      if (seen.has(c.id)) continue;
      seen.add(c.id);
      if (candidates.includes(c.name)) {
        channelCache.set(dealId, c.id);
        return c.id;
      }
    }
    if (!resp.response_metadata?.next_cursor) break;
    cursor = resp.response_metadata.next_cursor;
  }

  return null;
}

/** Determine channel to use for a work order */
async function resolveChannelId({ dealId }) {
  // 1) Deal-specific channel by name
  const byDeal = await findDealChannelId(dealId);
  if (byDeal) return byDeal;

  // 2) Fallback default
  return DEFAULT_CHANNEL;
}

/** Optional: get DM user (assigned lead) */
function resolveAssigneeSlackId({ productionTeamId, productionTeamName }) {
  const name =
    productionTeamName ||
    (productionTeamId ? PRODUCTION_TEAM_MAP[productionTeamId] : null);
  if (!name) return null;
  return SLACK_USER_IDS[name] || null;
}

/* ========= Build Work Order PDF (returns Buffer) ========= */
async function buildWorkOrderPdfBuffer({ activity, dealTitle, typeOfService, location, channelForQR }) {
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
  doc.image(qrBuffer, { fit: [180, 180] });
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
  form.append('file', pdfBuffer, {
    filename: filename || 'workorder.pdf',
    contentType: 'application/pdf',
  });
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

/* ========= Pipedrive webhook → Slack + PDF + PD file ========= */
expressApp.post('/pipedrive-task', async (req, res) => {
  if (!PD_WEBHOOK_KEY || req.query.key !== PD_WEBHOOK_KEY) {
    return res.status(403).send('Forbidden');
  }

  try {
    const activity = req.body?.data || req.body?.current || null; // supports PD v1/v2 payloads
    if (!activity) return res.status(200).send('No activity.');

    // Fetch full activity details
    const aRes = await fetch(
      `https://api.pipedrive.com/v1/activities/${encodeURIComponent(activity.id)}?api_token=${PIPEDRIVE_API_TOKEN}`
    );
    const aJson = await aRes.json();

    let fullNote = '_No note provided_';
    let dealId = 'N/A';
    let dealTitle = 'N/A';
    let typeOfService = 'N/A';
    let location = 'N/A';
    let productionTeamId = null;

    if (aJson?.success && aJson.data) {
      const data = aJson.data;
      fullNote = (data.note || fullNote).replace(/<br\/?>(\s)?/g, '\n').replace(/&nbsp;/g, ' ').trim();
      dealId = data.deal_id || 'N/A';

      if (dealId && dealId !== 'N/A') {
        const dRes = await fetch(
          `https://api.pipedrive.com/v1/deals/${encodeURIComponent(dealId)}?api_token=${PIPEDRIVE_API_TOKEN}`
        );
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) {
          const deal = dJson.data;
          dealTitle = deal.title || 'N/A';
          const serviceId = deal['5b436b45b63857305f9691910b6567351b5517bc']; // Type of Service custom field
          typeOfService = SERVICE_MAP[serviceId] || serviceId || 'N/A';
          location = deal.location || 'N/A';

          // If you store the Production Team enum on the deal, capture it here (replace key if different):
          // productionTeamId = deal['<your_production_team_field_key>'] || null;
        }
      }
    }

    // Decide channel for this WO
    const slackChannelId = await resolveChannelId({ dealId });
    console.log(`[WO] using Slack channel ${slackChannelId || DEFAULT_CHANNEL} for deal ${dealId}`);

    // Optional DM to assigned lead (disabled by default; enable if desired)
    const assigneeSlackId = resolveAssigneeSlackId({
      productionTeamId,
      productionTeamName: productionTeamId ? PRODUCTION_TEAM_MAP[productionTeamId] : 'Mike', // default to Mike while testing
    });

    // Post Slack task card
    const message = {
      channel: slackChannelId || DEFAULT_CHANNEL,
      text: `📌 *New Task*\n• *${activity.subject}*\n🗕️ Due: ${activity.due_date || 'No due date'}\n📜 Note: ${fullNote}\n🏷️ Deal ID: ${dealId} - *${dealTitle}*\n📦 Type of Service: ${typeOfService}\n📍 Location: ${location}\n✅ _Click the checkbox below to complete_`,
      blocks: [
        {
          type: 'section',
          text: {
            type: 'mrkdwn',
            text:
              `📌 *New Task*\n• *${activity.subject}*` +
              `\n🗕️ Due: ${[activity.due_date, activity.due_time].filter(Boolean).join(' ') || 'No due date'}` +
              `\n📜 Note: ${fullNote}` +
              `\n🏷️ Deal ID: ${dealId} - *${dealTitle}*` +
              `\n📦 Type of Service: ${typeOfService}` +
              `\n📍 Location: ${location}`,
          },
        },
        {
          type: 'actions',
          elements: [
            {
              type: 'checkboxes',
              action_id: 'complete_task',
              options: [
                {
                  text: { type: 'mrkdwn', text: 'Mark as complete' },
                  value: `task_${activity.id}`,
                },
              ],
            },
          ],
        },
      ],
    };

    await app.client.chat.postMessage(message);
    console.log('[WO] task card posted');

    // Build & upload PDF (Slack + Pipedrive)
    try {
      console.log('[WO] building PDF for activity', activity.id);
      const pdfBuffer = await buildWorkOrderPdfBuffer({
        activity: aJson.data,
        dealTitle,
        typeOfService,
        location,
        channelForQR: slackChannelId || DEFAULT_CHANNEL,
      });

      const safe = (s) => (s || '').toString().replace(/[^\w\-]+/g, '_').slice(0, 60);
      const filename = `WO_${safe(dealTitle)}_${safe(activity.subject)}.pdf`;

      console.log('[WO] uploading to Slack channel', slackChannelId || DEFAULT_CHANNEL, 'filename', filename);
      await uploadPdfToSlack({
        channel: slackChannelId || DEFAULT_CHANNEL,
        filename,
        pdfBuffer,
        title: `Work Order — ${activity.subject || ''}`,
        initialComment: '📄 Work Order PDF (scan QR to complete)',
      });
      console.log('[WO] Slack upload done');

      console.log('[WO] uploading to Pipedrive deal', dealId);
      await uploadPdfToPipedrive({ dealId, pdfBuffer, filename });
      console.log('[WO] PD file upload done');

      // Optional PD note with the QR-complete link
      const completeUrl = makeSignedCompleteUrl({
        aid: String(activity.id),
        did: dealId !== 'N/A' ? String(dealId) : '',
        cid: slackChannelId || DEFAULT_CHANNEL,
      });
      console.log('[WO] posting PD note with QR link');
      await fetch(`https://api.pipedrive.com/v1/notes?api_token=${PIPEDRIVE_API_TOKEN}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ deal_id: dealId, content: `Work Order PDF attached.\nScan to complete: ${completeUrl}` }),
      });
      console.log('[WO] PD note posted');

      // Optional: DM the assigned production lead a copy (enable if wanted)
      if (assigneeSlackId) {
        try {
          await app.client.chat.postMessage({
            channel: assigneeSlackId,
            text: `You were assigned a new Work Order for deal ${dealId} — *${dealTitle}*.`,
          });
          await uploadPdfToSlack({
            channel: assigneeSlackId,
            filename,
            pdfBuffer,
            title: `Work Order — ${activity.subject || ''}`,
            initialComment: '📄 Work Order PDF (scan QR to complete)',
          });
          console.log('[WO] DM sent to lead', assigneeSlackId);
        } catch (dmErr) {
          console.warn('DM to assignee failed (ok if disabled/no scope):', dmErr?.data || dmErr?.message || dmErr);
        }
      }
    } catch (e) {
      console.error('PDF/QR upload failed:', e);
    }

    res.status(200).send('OK');
  } catch (error) {
    console.error('Webhook error:', error);
    res.status(500).send('Server error.');
  }
});

/* ========= QR scan → complete task in Pipedrive (+ Slack ping) ========= */
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

    const channel = cid || DEFAULT_CHANNEL;
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
    console.error('DEBUG /upload-test error:', e.data || e);
    res.status(500).send(`upload failed: ${e.data ? JSON.stringify(e.data) : e.message}`);
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

    let dealTitle = 'N/A',
      typeOfService = 'N/A',
      location = 'N/A';
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
      }
    }

    const pdfBuffer = await buildWorkOrderPdfBuffer({
      activity: data,
      dealTitle,
      typeOfService,
      location,
      channelForQR: await resolveChannelId({ dealId }),
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
