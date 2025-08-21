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
const SIGNING_SECRET = process.env.WO_QR_SECRET;
const DEFAULT_CHANNEL = process.env.DEFAULT_SLACK_CHANNEL_ID || 'C098H8GU355';
const BASE_URL = process.env.BASE_URL;
const PD_WEBHOOK_KEY = process.env.PD_WEBHOOK_KEY; // ?key=...
const ALLOW_DEFAULT_FALLBACK = process.env.ALLOW_DEFAULT_FALLBACK !== 'false';
const FORCE_CHANNEL_ID = process.env.FORCE_CHANNEL_ID || null; // debugging override

// Feature toggles
const ENABLE_PD_FILE_UPLOAD     = process.env.ENABLE_PD_FILE_UPLOAD     !== 'false';
const ENABLE_PD_NOTE            = process.env.ENABLE_PD_NOTE            !== 'false';
const ENABLE_SLACK_PDF_UPLOAD   = process.env.ENABLE_SLACK_PDF_UPLOAD   !== 'false';
const ENABLE_DELETE_ON_REASSIGN = process.env.ENABLE_DELETE_ON_REASSIGN !== 'false';

if (!SLACK_SIGNING_SECRET) throw new Error('Missing SLACK_SIGNING_SECRET');
if (!SLACK_BOT_TOKEN) throw new Error('Missing SLACK_BOT_TOKEN');
if (!PIPEDRIVE_API_TOKEN) throw new Error('Missing PIPEDRIVE_API_TOKEN');
if (!SIGNING_SECRET) throw new Error('Missing WO_QR_SECRET');
if (!BASE_URL) throw new Error('Missing BASE_URL');
if (!PD_WEBHOOK_KEY) console.warn('⚠️ PD_WEBHOOK_KEY not set; /pipedrive-task will 403.');

// Crash handlers
process.on('unhandledRejection', (r)=>console.error('[FATAL] Unhandled Rejection:', r?.stack||r));
process.on('uncaughtException', (e)=>console.error('[FATAL] Uncaught Exception:', e?.stack||e));

/* ========= Dictionaries ========= */
const SERVICE_MAP = { 27:'Water Mitigation',28:'Fire Cleanup',29:'Contents',30:'Biohazard',31:'General Cleaning',32:'Duct Cleaning' };
const PRODUCTION_TEAM_MAP = { 47:'Kings',48:'Johnathan',49:'Penabad',50:'Hector',51:'Sebastian',52:'Anastacio',53:'Mike',54:'Gary',55:'Greg',56:'Amber',57:'Anna Marie',58:'Slot 3',59:'Slot 4',60:'Slot 5' };

// PD custom field key (Production Team on DEAL/ACTIVITY)
const PRODUCTION_TEAM_FIELD_KEY = '8bbab3c120ade3217b8738f001033064e803cdef';

// Production Team enum ID → Slack channel
const PRODUCTION_TEAM_TO_CHANNEL = {
  47: 'C09BXCCD95W',   // Kings
  48: 'C09ASB1N32B',   // Johnathan
  49: 'C09ASBE36Q7',   // Penabad
  50: 'C09B6P5LVPY',   // Hector
  51: 'C09AZ6VT459',   // Sebastian
  52: 'C09BA0XUAV7',   // Anastacio
  53: 'C098H8GU355',   // Mike
  54: 'C09AZ63JEJF',   // Gary
  55: 'C09BFFGBYTB',   // Greg
  56: 'C09B49MJHEE',   // Amber (Slot 1)
  57: 'C09B85LE544',   // Anna Marie (Slot 2)
  58: null,            // Slot 3
  59: null,            // Slot 4
  60: null             // Slot 5
};

// Fallback name→channel for parsing "Crew: Name"
const NAME_TO_CHANNEL = {
  anastacio:'C09BA0XUAV7',
  mike:'C098H8GU355',
  greg:'C09BFFGBYTB',
  gregory:'C09BFFGBYTB', // alias
  amber:'C09B49MJHEE',
  'anna marie':'C09B85LE544',
  annamarie:'C09B85LE544',
  'anna-marie':'C09B85LE544', // alias with hyphen
  kings:'C09BXCCD95W',
  penabad:'C09ASBE36Q7',
  johnathan:'C09ASB1N32B',
  gary:'C09AZ63JEJF',
  hector:'C09B6P5LVPY',
  sebastian:'C09AZ6VT459'
};
const NAME_TO_TEAM_ID = {
  anastacio:52,
  mike:53,
  greg:55,
  gregory:55,
  amber:56,
  'anna marie':57,
  annamarie:57,
  'anna-marie':57,
  kings:47,
  penabad:49,
  johnathan:48,
  gary:54,
  hector:50,
  sebastian:51
};

/* ========= Slack App ========= */
const receiver = new ExpressReceiver({
  signingSecret: SLACK_SIGNING_SECRET,
  endpoints: { events:'/slack/events', commands:'/slack/commands', actions:'/slack/interact' },
  processBeforeResponse: true,
});
const app = new App({ token: SLACK_BOT_TOKEN, signingSecret: SLACK_SIGNING_SECRET, receiver });

app.event('app_mention', async ({ event, say }) => {
  await say(`Hey <@${event.user}>, Dispatcher is online and running!`);
});

// ⚠️ QR-ONLY COMPLETION: removed crew completion checkbox handler
// (Old app.action('complete_task') deleted to enforce QR-only path.)

/* ========= Express ========= */
const expressApp = receiver.app;
expressApp.use(express.json());
expressApp.get('/', (_req,res)=>res.status(200).send('Dispatcher OK'));
expressApp.get('/healthz', (_req,res)=>res.status(200).send('ok'));

/* ========= Helpers ========= */
const b64url = (b)=>Buffer.from(b).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
const sign = (raw)=>b64url(crypto.createHmac('sha256', SIGNING_SECRET).update(raw).digest());
function verify(raw,sig){
  try{
    const a=Buffer.from(sign(raw)), b=Buffer.from(String(sig));
    if(a.length!==b.length) return false;
    return crypto.timingSafeEqual(a,b);
  }catch{ return false; }
}
const cleanBase = ()=> String(BASE_URL||'').trim().replace(/^=+/, '');
const titleize = (s)=> String(s||'').split(' ').map(w=>w? (w[0].toUpperCase()+w.slice(1)) : '').join(' ');

// Minimal HTML→text for PD notes
function htmlToPlainText(input=''){
  let s = String(input);
  s = s.replace(/<br\s*\/?>(\s)?/gi, '\n');
  s = s.replace(/<\/?p[^>]*>/gi, '\n');
  s = s.replace(/<\/li>\s*<li[^>]*>/gi, '\n• ');
  s = s.replace(/<li[^>]*>/gi, '• ');
  s = s.replace(/<\/li>/gi, '');
  s = s.replace(/<\/?ul[^>]*>/gi, '\n');
  s = s.replace(/<\/?ol[^>]*>/gi, '\n');
  s = s.replace(/<[^>]+>/g, '');
  s = s.replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&lt;/g, '<')
       .replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'");
  s = s.replace(/\n{3,}/g, '\n\n').trim();
  return s;
}

// Crew name extract + normalize (robust to punctuation/aliases)
function crewFromString(s){
  if(!s) return null;
  const m = String(s).match(/crew\s*:\s*([^\n\r]+)/i);
  if (!m || !m[1]) return null;
  let v = m[1].split(/[|,·•\-\u2013\u2014]/)[0];
  v = v.replace(/\(.*?\)/g,'');
  v = v.replace(/[^A-Za-z\s]/g,'');
  return v.replace(/\s+/g,' ').trim();
}
function normalizeCrewName(s){
  if(!s) return null;
  let x = String(s).toLowerCase();
  x = x.replace(/\s+/g,' ').trim();
  if (x === 'anna') x = 'anna marie';
  if (x === 'anna-marie') x = 'anna marie';
  if (x === 'gregory') x = 'greg';
  if (x === 'jonathan') x = 'johnathan';
  return x;
}

// PD time normalization
function getTimeField(v){ if(!v) return ''; if(typeof v==='string') return v; if(typeof v==='object' && v.value) return String(v.value); return ''; }

/* ======== Scheduling guards (to stop early/duplicate posts) ======== */
const hasDue = (a)=> !!(a?.due_date || getTimeField(a?.due_time));
const becameScheduled = (prev, curr)=> !hasDue(prev) && hasDue(curr);

// Ignore billing-type tasks entirely (subject or type contains billed/billing/invoice)
const isBillingTask = (a) => {
  const s = String(a?.subject || '').toLowerCase();
  const t = String(a?.type || '').toLowerCase();
  return /\bbilled\b|\bbilling\b|\binvoice\b/.test(s) || /\bbilled\b|\bbilling\b|\binvoice\b/.test(t);
};

// In-memory idempotency: track what we've posted to avoid double posts from near-simultaneous hooks
const POSTED_ACTIVITIES = new Map(); // activityId -> timestamp ms
function alreadyPosted(aid, ttlMs = 5 * 60 * 1000){ // 5 minutes window
  const now = Date.now();
  const last = POSTED_ACTIVITIES.get(String(aid)) || 0;
  if (last && (now - last) < ttlMs) return true;
  POSTED_ACTIVITIES.set(String(aid), now);
  return false;
}

/* ========= Channel resolution ========= */
const channelCache = new Map(); // dealId -> channelId

async function findDealChannelId(dealId){
  if (!dealId) return null;
  const key = String(dealId);
  if (channelCache.has(key)) return channelCache.get(key);

  const tokens = [`deal${key}`,'deal-'+key,'deal_'+key, `${key}`, `${key}-deal`, `${key}_deal`, `job${key}`,'job-'+key,'job_'+key];
  let cursor, seen = new Set();
  while (true){
    const resp = await app.client.conversations.list({ types:'public_channel,private_channel', limit:1000, cursor, exclude_archived:true });
    for (const c of (resp.channels||[])){
      if (!c?.id || !c?.name) continue;
      if (seen.has(c.id)) continue; seen.add(c.id);
      const exact = tokens.includes(c.name);
      const suffix = tokens.some(t => c.name.endsWith(t));
      const strip = s => String(s).toLowerCase().replace(/[^a-z0-9]/g,'');
      const fuzzy = strip(c.name).includes(strip(`deal${key}`));
      if (exact || suffix || fuzzy){
        channelCache.set(key, c.id);
        return c.id;
      }
    }
    if (!resp.response_metadata?.next_cursor) break;
    cursor = resp.response_metadata.next_cursor;
  }
  return null;
}

async function ensureBotInChannel(channelId){
  if (!channelId) return;
  try{ await app.client.conversations.join({ channel: channelId }); }
  catch{ /* already_in_channel etc. */ }
}

async function resolveDealChannelId({ dealId, allowDefault = ALLOW_DEFAULT_FALLBACK }){
  const byDeal = await findDealChannelId(dealId);
  if (byDeal) return byDeal;
  return allowDefault ? DEFAULT_CHANNEL : null;
}

/* ========= Assignee detection (Deal OR Activity OR Crew:) ========= */
function detectAssignee({ deal, activity }){
  // 1) Activity-level Production Team
  if (activity) {
    const aTid = activity[PRODUCTION_TEAM_FIELD_KEY];
    if (aTid && PRODUCTION_TEAM_TO_CHANNEL[aTid]) {
      return { teamId: aTid, teamName: PRODUCTION_TEAM_MAP[aTid] || `Team ${aTid}`, channelId: PRODUCTION_TEAM_TO_CHANNEL[aTid] };
    }
  }
  // 2) Deal-level Production Team
  if (deal) {
    const dTid = deal[PRODUCTION_TEAM_FIELD_KEY];
    if (dTid && PRODUCTION_TEAM_TO_CHANNEL[dTid]) {
      return { teamId: dTid, teamName: PRODUCTION_TEAM_MAP[dTid] || `Team ${dTid}`, channelId: PRODUCTION_TEAM_TO_CHANNEL[dTid] };
    }
  }
  // 3) Fallback parse from title/subject → robust & normalized
  const rawName = crewFromString(deal?.title) || crewFromString(activity?.subject);
  if (rawName){
    const key = normalizeCrewName(rawName);
    const channelId = NAME_TO_CHANNEL[key] || null;
    const teamId = NAME_TO_TEAM_ID[key] || null;
    const teamName = PRODUCTION_TEAM_MAP[teamId] || titleize(key);
    if (channelId) return { teamId, teamName, channelId };
    console.warn('[WO] Crew fallback matched name but not mapped to channel:', rawName);
  }
  return { teamId:null, teamName:null, channelId:null };
}

/* ========= Reassignment & completion tracking =========
   activityId -> { assigneeChannelId, messageTs, fileIds: string[] }
*/
const ASSIGNEE_POSTS = new Map();
const AID_TAG = (id)=>`[AID:${id}]`;

/* ========= PDF builders ========= */
async function buildWorkOrderPdfBuffer({ activity, dealTitle, typeOfService, location, channelForQR, assigneeName }) {
  const completeUrl = makeSignedCompleteUrl({
    aid: String(activity.id),
    did: activity.deal_id ? String(activity.deal_id) : '',
    cid: channelForQR || DEFAULT_CHANNEL,
  });

  const qrDataUrl = await QRCode.toDataURL(completeUrl);
  const qrBuffer = Buffer.from(qrDataUrl.replace(/^data:image\/png;base64,/,'') ,'base64');

  const doc = new PDFDocument({ margin: 36 });
  const chunks = [];
  doc.on('data', c => chunks.push(c));
  const done = new Promise(r => doc.on('end', ()=>r(Buffer.concat(chunks))));

  doc.fontSize(20).text('Work Order', { align:'center' });
  doc.moveDown(0.25);
  doc.fontSize(10).fillColor('#666').text(new Date().toLocaleString(), { align:'center' });
  doc.moveDown(1);

  const scheduled = [activity.due_date, getTimeField(activity.due_time)].filter(Boolean).join(' ').trim();

  doc.fillColor('#000').fontSize(12);
  doc.text(`Task: ${activity.subject || '-'}`);
  doc.text(`Due:  ${scheduled || '-'}`);
  doc.text(`Deal: ${dealTitle || '-'}`);
  doc.text(`Type of Service: ${typeOfService || '-'}`);
  doc.text(`Location: ${location || '-'}`);
  if (assigneeName) doc.text(`Assigned To: ${assigneeName}`);
  doc.moveDown(0.5);

  const rawNote = htmlToPlainText(activity.note || '');
  if (rawNote) {
    doc.font('Helvetica-Bold').text('Scope / Notes');
    doc.font('Helvetica').text(rawNote, { width: 520 });
    doc.moveDown(0.5);
  }

  doc.font('Helvetica-Bold').text('Scan to Complete');
  doc.font('Helvetica').fontSize(10).fillColor('#555').text('Scanning marks this task complete in Pipedrive.');
  doc.moveDown(0.5);
  doc.image(qrBuffer, { fit:[120,120] });
  doc.moveDown(0.25);
  doc.fontSize(8).fillColor('#777').text(completeUrl, { width: 520 });

  doc.end();
  return done;
}

function buildCompletionPdfBuffer({ activity, dealTitle, typeOfService, location, completedAt }) {
  const doc = new PDFDocument({ margin: 50 });
  const chunks = [];
  doc.on('data', c => chunks.push(c));
  const finish = new Promise(r => doc.on('end', () => r(Buffer.concat(chunks))));

  doc.fontSize(20).text('Work Order Completed', { align: 'center' });
  doc.moveDown();
  doc.fontSize(12)
     .text(`Task: ${activity?.subject || '-'}`)
     .text(`Activity ID: ${activity?.id || '-'}`)
     .text(`Deal: ${dealTitle || '-'}`)
     .text(`Type of Service: ${typeOfService || '-'}`)
     .text(`Location: ${location || '-'}`)
     .text(`Completed At: ${new Date(completedAt || Date.now()).toLocaleString()}`);

  const rawNote = htmlToPlainText(activity?.note || '');
  if (rawNote) {
    doc.moveDown();
    doc.font('Helvetica-Bold').text('Final Notes');
    doc.font('Helvetica').text(rawNote, { width: 520 });
  }

  doc.end();
  return finish;
}

/* ========= Slack helpers ========= */
function buildSlackBlocks({ activity, noteText, dealId, dealTitle, typeOfService, location, assigneeName }){
  const scheduledText = [activity.due_date, getTimeField(activity.due_time)].filter(Boolean).join(' ') || 'No due date';
  const pdfUrl = buildPdfUrl(activity.id);

  const actionElements = [];
  if (pdfUrl) actionElements.push({ type:'button', text:{type:'plain_text', text:'Open Work Order PDF'}, url: pdfUrl });

  const cleanNote = htmlToPlainText(noteText || activity.note || '_No note provided_');

  return [
    { type:'section', text:{ type:'mrkdwn', text:
      `📌 *New Task*\n• *${activity.subject || '-'}*` +
      `\n🗕️ Due: ${scheduledText}` +
      `\n📜 Note:\n${cleanNote}` +
      `\n🏷️ Deal ID: ${dealId} - *${dealTitle}*` +
      `\n📦 Type of Service: ${typeOfService}` +
      `\n📍 Location: ${location}` +
      (assigneeName ? `\n👷 Assigned To: ${assigneeName}` : '') +
      `\n\n*Scan the QR on the PDF to complete this task.*`
    }},
    ...(actionElements.length ? [{ type:'actions', elements: actionElements }] : [])
  ];
}

function buildPdfUrl(aid){
  try{
    const base = new URL(cleanBase());
    const u = new URL('/wo/pdf', base);
    u.searchParams.set('aid', String(aid));
    const href = u.toString();
    if (!/^https?:\/\//i.test(href)) return null;
    return href;
  }catch(e){ console.error('[WO] buildPdfUrl error:', e?.message||e); return null; }
}

async function uploadPdfToSlack({ channel, filename, pdfBuffer, title, initialComment='' }){
  return app.client.files.uploadV2({
    channel_id: channel,
    file: pdfBuffer,
    filename,
    title: title||filename,
    initial_comment: initialComment
  });
}

async function uploadPdfToPipedrive({ dealId, pdfBuffer, filename }){
  if(!dealId || dealId==='N/A') return;
  const form = new FormData();
  form.append('deal_id', String(dealId));
  form.append('file', pdfBuffer, { filename: filename || 'workorder.pdf', contentType: 'application/pdf' });
  const resp = await fetch(`https://api.pipedrive.com/v1/files?api_token=${PIPEDRIVE_API_TOKEN}`, { method:'POST', headers: form.getHeaders(), body: form });
  const j = await resp.json();
  if(!j?.success){ console.error('PD file upload failed:', j); throw new Error('PD file upload failed'); }
  return j;
}

/* ========= Delete helpers ========= */
async function deleteAssigneePdfByMarker(activityId, channelId, lookback=400){
  try{
    const h = await app.client.conversations.history({ channel: channelId, limit: lookback, inclusive: true });
    for (const m of (h?.messages||[])){
      const text = (m.text||'') + ' ' + (m.previous_message?.text||'');
      if (text.includes(AID_TAG(activityId))){
        const files = m.files || [];
        for (const f of files){
          try{ await app.client.files.delete({ file: f.id }); } catch(e){ console.warn('[WO] files.delete (marker) failed', f.id, e?.data||e?.message||e); }
        }
        try{ await app.client.chat.delete({ channel: channelId, ts: m.ts }); } catch(e){ /* ignore */ }
      }
    }
  }catch(e){ console.warn('[WO] deleteAssigneePdfByMarker failed', channelId, e?.data||e?.message||e); }
}

async function deleteAssigneePost(activityId){
  const key = String(activityId);
  const rec = ASSIGNEE_POSTS.get(key);
  if (rec){
    const { assigneeChannelId, messageTs, fileIds } = rec;
    if (assigneeChannelId && messageTs){
      try { await app.client.chat.delete({ channel: assigneeChannelId, ts: messageTs }); } catch (e){ /* ignore */ }
      await deleteAssigneePdfByMarker(activityId, assigneeChannelId, 400);
    }
    if (Array.isArray(fileIds)){
      for (const fid of fileIds){ if (!fid) continue; try { await app.client.files.delete({ file: fid }); } catch(e){} }
    }
    ASSIGNEE_POSTS.delete(key);
  }
}

async function findAndDeleteActivityMessageInChannel(activityId, channelId, lookback=200){
  try{
    const hist = await app.client.conversations.history({ channel: channelId, limit: lookback, inclusive: true });
    const msgs = hist?.messages || [];
    for (const m of msgs){
      const blocks = m.blocks || [];
      const hasOurCheckbox = blocks.some(b =>
        b?.type === 'actions' &&
        Array.isArray(b.elements) &&
        b.elements.some(el => el?.type === 'checkboxes' &&
          Array.isArray(el.options) &&
          el.options.some(opt => opt?.value === `task_${activityId}`)));
      if (hasOurCheckbox){
        try { await app.client.chat.delete({ channel: channelId, ts: m.ts }); } catch(e){}
      }
    }
  }catch(e){
    console.warn('[WO] findAndDeleteActivityMessageInChannel failed', channelId, e?.data || e?.message || e);
  }
  await deleteAssigneePdfByMarker(activityId, channelId, 400);
  return false;
}

function channelFromOldAssignment({ oldTeamId, oldCrewName }){
  if (oldTeamId && PRODUCTION_TEAM_TO_CHANNEL[oldTeamId]) return PRODUCTION_TEAM_TO_CHANNEL[oldTeamId];
  if (oldCrewName){
    const key = normalizeCrewName(String(oldCrewName));
    if (NAME_TO_CHANNEL[key]) return NAME_TO_CHANNEL[key];
  }
  return null;
}

/* ========= Core post ========= */
async function postWorkOrderToChannels({ activity, deal, jobChannelId, assigneeChannelId, assigneeName, noteText }){
  const dealId = activity.deal_id || (deal?.id ?? 'N/A');
  const dealTitle = deal?.title || 'N/A';
  const serviceId = deal ? deal['5b436b45b63857305f9691910b6567351b5517bc'] : null;
  const typeOfService = SERVICE_MAP[serviceId] || serviceId || 'N/A';
  const location = deal?.location || 'N/A';

  const pdfBuffer = await buildWorkOrderPdfBuffer({
    activity, dealTitle, typeOfService, location,
    channelForQR: jobChannelId || DEFAULT_CHANNEL, assigneeName
  });

  const safe = (s) => (s || '').toString().replace(/[^\w\-]+/g,'_').slice(0,60);
  const filename = `WO_${safe(dealTitle)}_${safe(activity.subject)}.pdf`;
  const scheduledText = [activity.due_date, getTimeField(activity.due_time)].filter(Boolean).join(' ') || 'No due date';
  const blocks = buildSlackBlocks({ activity, noteText, dealId, dealTitle, typeOfService, location, assigneeName });
  const ic = `📄 Work Order PDF (scan QR to complete) ${AID_TAG(activity.id)}`;

  if (jobChannelId){
    await ensureBotInChannel(jobChannelId);
    await app.client.chat.postMessage({ channel: jobChannelId, text: `📌 New Task • ${activity.subject || '-'} • Due: ${scheduledText}`, blocks });
    if (ENABLE_SLACK_PDF_UPLOAD){
      try { await uploadPdfToSlack({ channel: jobChannelId, filename, pdfBuffer, title:`Work Order — ${activity.subject || ''}`, initialComment: ic }); }
      catch(e){ console.warn('[WO] files.uploadV2 (job) failed:', e?.data || e?.message || e); }
    }
  }

  if (assigneeChannelId){
    await ensureBotInChannel(assigneeChannelId);

    if (ENABLE_DELETE_ON_REASSIGN) { await deleteAssigneePost(activity.id); }

    const aMsg = await app.client.chat.postMessage({ channel: assigneeChannelId, text: `📌 New Task • ${activity.subject || '-'} • Due: ${scheduledText}`, blocks });

    const record = { assigneeChannelId, messageTs: aMsg.ts, fileIds: [] };

    if (ENABLE_SLACK_PDF_UPLOAD){
      try {
        const up = await uploadPdfToSlack({ channel: assigneeChannelId, filename, pdfBuffer, title:`Work Order — ${activity.subject || ''}`, initialComment: ic });
        const fids = [];
        if (up?.files && Array.isArray(up.files)) { for (const f of up.files) if (f?.id) fids.push(f.id); }
        else if (up?.file?.id) { fids.push(up.file.id); }
        record.fileIds = fids;
      } catch(e){ console.warn('[WO] files.uploadV2 (assignee) failed:', e?.data || e?.message || e); }
    }

    ASSIGNEE_POSTS.set(String(activity.id), record);
  }

  // Attach WO PDF to PD and leave a note (no completion link)
  if (ENABLE_PD_FILE_UPLOAD){ try{ await uploadPdfToPipedrive({ dealId, pdfBuffer, filename }); } catch(e){ console.error('[WO] PD upload failed:', e); } }
  if (ENABLE_PD_NOTE){
    await fetch(`https://api.pipedrive.com/v1/notes?api_token=${PIPEDRIVE_API_TOKEN}`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ deal_id: dealId, content: `Work Order PDF posted to Slack and attached in Pipedrive.` }) });
  }
}

/* ========= Webhook (activities + deal updates + deletes) ========= */
expressApp.post('/pipedrive-task', async (req, res) => {
  try {
    if (!PD_WEBHOOK_KEY || req.query.key !== PD_WEBHOOK_KEY) { return res.status(403).send('Forbidden'); }

    const meta = req.body?.meta || {};
    const rawEntity  = (meta.entity || '').toString().toLowerCase();
    const rawAction  = (meta.action || meta.event || meta.change || '').toString().toLowerCase();

    const action = /^(create|add|added)$/.test(rawAction) ? 'create'
                  : /^(update|updated|change|changed)$/.test(rawAction) ? 'update'
                  : /^(delete|deleted)$/.test(rawAction) ? 'delete'
                  : rawAction || 'unknown';

    const entity = rawEntity || 'unknown';
    const data   = req.body?.data || req.body?.current || null;
    const prev   = req.body?.previous || null;

    console.log('[PD Hook] entity=%s action=%s id=%s', entity, action, data?.id || meta?.entity_id || 'n/a');

    // 🔒 NEVER post on deal create; only handle deal UPDATE for re-routing existing open activities
    if (entity === 'deal' && action === 'update') {
      const deal = data;
      if (!deal?.id) return res.status(200).send('No deal');

      const oldTeamId = prev ? prev[PRODUCTION_TEAM_FIELD_KEY] : undefined;
      const newTeamId = deal[PRODUCTION_TEAM_FIELD_KEY];
      const oldCrewNameRaw = crewFromString(prev?.title);
      const newCrewNameRaw = crewFromString(deal?.title);

      const teamChanged = oldTeamId !== newTeamId;
      const crewChanged = (normalizeCrewName(oldCrewNameRaw) || '') !== (normalizeCrewName(newCrewNameRaw) || '');
      console.log('[PD Hook] DEAL update teamChanged=%s crewChanged=%s oldTeam=%s newTeam=%s oldCrew=%s newCrew=%s', teamChanged, crewChanged, oldTeamId, newTeamId, oldCrewNameRaw, newCrewNameRaw);
      if (!teamChanged && !crewChanged) return res.status(200).send('OK');

      const assignee = detectAssignee({ deal, activity: null });
      if (!assignee.channelId) { console.log('[PD Hook] no assignee channel resolved for deal update'); return res.status(200).send('OK'); }

      let jobChannelId = await resolveDealChannelId({ dealId: deal.id, allowDefault: ALLOW_DEFAULT_FALLBACK });
      if (FORCE_CHANNEL_ID) jobChannelId = FORCE_CHANNEL_ID;

      const listRes = await fetch(`https://api.pipedrive.com/v1/activities?deal_id=${encodeURIComponent(deal.id)}&done=0&start=0&limit=50&api_token=${PIPEDRIVE_API_TOKEN}`);
      const listJson = await listRes.json();
      const items = (listJson?.data || []).filter(a => a && (a.done === false || a.done === 0) && hasDue(a) && !isBillingTask(a));
      console.log('[PD Hook] DEAL update → repost %d open scheduled activities', items.length);

      const oldChannel = channelFromOldAssignment({ oldTeamId, oldCrewName: oldCrewNameRaw });

      for (const activity of items) {
        try {
          if (ENABLE_DELETE_ON_REASSIGN) {
            if (oldChannel) await findAndDeleteActivityMessageInChannel(activity.id, oldChannel, 200);
            await deleteAssigneePost(activity.id);
          }
          if (ASSIGNEE_POSTS.has(String(activity.id))) { console.log('[WO] skip repost; already posted in current runtime', activity.id); continue; }
          await postWorkOrderToChannels({ activity, deal, jobChannelId, assigneeChannelId: assignee.channelId, assigneeName: assignee.teamName, noteText: activity.note });
        } catch (e) { console.error('[WO] re-route failed', activity?.id, e?.message||e); }
      }

      return res.status(200).send('OK');
    }

    if (entity === 'activity' && (action === 'create' || action === 'update')) {
      if (!data?.id) return res.status(200).send('No activity');
      if (data.done === true || data.done === 1) { console.log('[PD Hook] skip activity id=%s (already done)', data.id); return res.status(200).send('OK'); }

      const aRes = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(data.id)}?api_token=${PIPEDRIVE_API_TOKEN}`);
      const aJson = await aRes.json();
      if (!aJson?.success || !aJson.data) return res.status(200).send('Activity fetch fail');
      const activity = aJson.data;
      if (activity.done === true || activity.done === 1) { console.log('[PD Hook] skip activity id=%s (done after fetch)', activity.id); return res.status(200).send('OK'); }
      if (isBillingTask(activity)) { console.log('[WO] skip activity id=%s (billing task)', activity.id); return res.status(200).send('OK'); }

      const scheduledNow = hasDue(activity);
      if (action === 'create' && !scheduledNow) {
        console.log('[WO] skip create; unscheduled activity id=%s', activity.id);
        return res.status(200).send('OK');
      }

      let deal = null;
      if (activity.deal_id) {
        const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(activity.deal_id)}?api_token=${PIPEDRIVE_API_TOKEN}`);
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) deal = dJson.data;
      }

      let allowPost = false;

      if (action === 'update') {
        const prevTeamId = prev ? prev[PRODUCTION_TEAM_FIELD_KEY] : undefined;
        const prevCrewNameRaw = crewFromString(prev?.subject);
        const newCrewNameRaw = crewFromString(activity?.subject);

        const newAss = detectAssignee({ deal, activity });
        const oldChannel = (()=>{
          if (prevTeamId && PRODUCTION_TEAM_TO_CHANNEL[prevTeamId]) return PRODUCTION_TEAM_TO_CHANNEL[prevTeamId];
          if (prevCrewNameRaw) { const key = normalizeCrewName(prevCrewNameRaw); return NAME_TO_CHANNEL[key] || null; }
          return null;
        })();

        const normalizedPrev = normalizeCrewName(prevCrewNameRaw);
        const normalizedNew  = normalizeCrewName(newCrewNameRaw);

        const reassigned =
          (prevTeamId !== undefined && prevTeamId !== activity[PRODUCTION_TEAM_FIELD_KEY]) ||
          (!!normalizedPrev && !!normalizedNew && normalizedPrev !== normalizedNew);

        const scheduleBecameReal = becameScheduled(prev, activity);

        if (reassigned && ENABLE_DELETE_ON_REASSIGN) {
          if (oldChannel) { console.log('[WO] activity.update → deleting old post in channel %s', oldChannel); await findAndDeleteActivityMessageInChannel(activity.id, oldChannel, 400); }
          await deleteAssigneePost(activity.id);
        }

        // Only post on update if reassigned OR it just became scheduled now
        allowPost = reassigned || scheduleBecameReal;
        if (!allowPost) {
          console.log('[WO] skip update; no reassignment and not newly scheduled. id=%s', activity.id);
          return res.status(200).send('OK');
        }
      }

      if (!ASSIGNEE_POSTS.has(String(activity.id)) && alreadyPosted(activity.id)) {
        console.log('[WO] drop duplicate within window id=%s', activity.id);
        return res.status(200).send('OK');
      }

      let jobChannelId = await resolveDealChannelId({ dealId: activity.deal_id, allowDefault: ALLOW_DEFAULT_FALLBACK });
      if (FORCE_CHANNEL_ID) jobChannelId = FORCE_CHANNEL_ID;
      const assignee = detectAssignee({ deal, activity });

      console.log('[PD Hook] ACTIVITY %s → deal=%s assigneeChannel=%s assignee=%s', action, activity.deal_id || 'N/A', assignee.channelId || 'none', assignee.teamName || 'unknown');

      await postWorkOrderToChannels({ activity, deal, jobChannelId, assigneeChannelId: assignee.channelId, assigneeName: assignee.teamName, noteText: activity.note });
      return res.status(200).send('OK');
    }

    if (entity === 'activity' && (action === 'delete')) {
      const id = data?.id || meta?.entity_id;
      console.log('[PD Hook] ACTIVITY delete id=%s → cleanup assignee post', id);
      if (id) await deleteAssigneePost(id);
      return res.status(200).send('OK');
    }

    console.log('[PD Hook] ignored event entity=%s action=%s', entity, action);
    return res.status(200).send('OK');
  } catch (error) {
    console.error('[PD Hook] ERROR:', error?.stack || error?.data || error?.message || error);
    return res.status(500).send('Server error.');
  }
});

/* ========= QR Complete (ONLY path to completion) ========= */
expressApp.get('/wo/complete', async (req, res) => {
  try {
    const { aid, did, cid, exp, sig } = req.query || {};
    if (!aid || !exp || !sig) return res.status(400).send('Missing params.');
    const now = Math.floor(Date.now()/1000);
    if (Number(exp) < now) return res.status(410).send('Link expired.');
    const raw = `${aid}.${did || ''}.${cid || ''}.${exp}`;
    if (!verify(raw, sig)) return res.status(403).send('Bad signature.');

    const pdResp = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(aid)}?api_token=${PIPEDRIVE_API_TOKEN}`, {
      method:'PUT', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ done:true, marked_as_done_time: new Date().toISOString() })
    });
    const pdJson = await pdResp.json();
    const ok = !!(pdJson && pdJson.success);

    let activity = null, deal = null, dealTitle='N/A', typeOfService='N/A', location='N/A';
    try {
      const aRes = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(aid)}?api_token=${PIPEDRIVE_API_TOKEN}`);
      const aJson = await aRes.json();
      if (aJson?.success && aJson.data) {
        activity = aJson.data;
        const dealId = did || activity.deal_id;
        if (dealId) {
          const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(dealId)}?api_token=${PIPEDRIVE_API_TOKEN}`);
          const dJson = await dRes.json();
          if (dJson?.success && dJson.data) {
            deal = dJson.data;
            dealTitle = deal.title || 'N/A';
            const serviceId = deal['5b436b45b63857305f9691910b6567351b5517bc'];
            typeOfService = SERVICE_MAP[serviceId] || serviceId || 'N/A';
            location = deal.location || 'N/A';
          }
        }
      }
    } catch (e) { console.warn('[WO] fetch for completion PDF failed', e?.message||e); }

    try {
      const completedPdf = await buildCompletionPdfBuffer({ activity, dealTitle, typeOfService, location, completedAt: Date.now() });
      const up = await uploadPdfToPipedrive({ dealId: did || activity?.deal_id, pdfBuffer: completedPdf, filename: `WO_${aid}_Completed.pdf` });
      console.log('[WO] completion PDF uploaded', up?.success);
    } catch (e) {
      console.error('[WO] completion PDF upload failed', e?.message||e);
    }

    await deleteAssigneePost(aid);

    const jobChannel = cid || DEFAULT_CHANNEL;
    if (jobChannel) {
      const text = ok ? `✅ Task *${aid}* marked complete in Pipedrive${did ? ` (deal ${did})` : ''}. PDF attached to deal.`
                      : `⚠️ Tried to complete task *${aid}* but Pipedrive didn’t confirm success.`;
      await app.client.chat.postMessage({ channel: jobChannel, text });
    }

    res.status(200).send(
      `<html><body style="font-family:Arial;padding:24px"><h2>Work Order Complete</h2>
       <p>Task <b>${aid}</b> ${ok ? 'has been updated' : 'could not be updated'} in Pipedrive.</p>
       ${did ? `<p>Deal: <b>${did}</b></p>` : ''}
       <p>${ok ? 'A completion PDF has been attached. ✅' : 'Please contact the office. ⚠️'}</p></body></html>`
    );
  } catch (err) {
    console.error('/wo/complete error:', err);
    res.status(500).send('Server error.');
  }
});

/* ========= Debug ========= */
expressApp.get('/debug/pdf', async (_req,res)=>{
  try{
    const doc = new PDFDocument({ margin:36 });
    const chunks=[]; doc.on('data',c=>chunks.push(c));
    doc.on('end',()=>{ const buf=Buffer.concat(chunks); res.setHeader('Content-Type','application/pdf'); res.setHeader('Content-Length', buf.length); res.send(buf); });
    doc.fontSize(20).text('Dispatcher PDF Test',{align:'center'});
    doc.moveDown();
    doc.fontSize(12).text('If you can read this, pdfkit is working on Railway.');
    doc.end();
  }catch(e){
    console.error('DEBUG /pdf error:', e);
    res.status(500).send('error');
  }
});

expressApp.get('/debug/upload-test', async (req,res)=>{
  try{
    const channel = req.query.cid || DEFAULT_CHANNEL;
    const doc = new PDFDocument({ margin:36 });
    const chunks=[]; doc.on('data',c=>chunks.push(c));
    const finished=new Promise(r=>doc.on('end',()=>r(Buffer.concat(chunks))));
    doc.fontSize(18).text('Dispatcher Slack Upload Test',{align:'center'});
    doc.moveDown().fontSize(12).text(`Channel: ${channel}`);
    doc.end();
    const pdfBuffer = await finished;
    const result = await app.client.files.uploadV2({ channel_id: channel, file: pdfBuffer, filename:'dispatcher-test.pdf', title:'Dispatcher Test PDF', initial_comment:`Test upload ${AID_TAG('debug')}` });
    console.log('DEBUG uploadV2 result:', result.ok);
    res.status(200).send('Uploaded test PDF to Slack with files.uploadV2.');
  }catch(e){
    console.error('DEBUG /upload-test error:', e?.data || e);
    res.status(500).send(`upload failed: ${e?.data ? JSON.stringify(e?.data) : e.message}`);
  }
});

/* ========= On-demand WO PDF ========= */
expressApp.get('/wo/pdf', async (req,res)=>{
  try{
    const aid = req.query.aid;
    if(!aid) return res.status(400).send('Missing aid');
    const aRes = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(aid)}?api_token=${PIPEDRIVE_API_TOKEN}`);
    const aj = await aRes.json();
    if (!aj?.success || !aj.data) return res.status(404).send('Activity not found');
    const data = aj.data;

    let dealTitle='N/A', typeOfService='N/A', location='N/A', assigneeName=null, deal=null;
    const dealId = data.deal_id;
    if (dealId){
      const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(dealId)}?api_token=${PIPEDRIVE_API_TOKEN}`);
      const dj = await dRes.json();
      if (dj?.success && dj.data){
        deal = dj.data;
        dealTitle = deal.title || 'N/A';
        const serviceId = deal['5b436b45b63857305f9691910b6567351b5517bc'];
        typeOfService = SERVICE_MAP[serviceId] || serviceId || 'N/A';
        location = deal.location || 'N/A';
        const ass = detectAssignee({ deal, activity: data });
        assigneeName = ass.teamName || assigneeName;
      }
    }

    const channelIdForQr = await resolveDealChannelId({ dealId, allowDefault: ALLOW_DEFAULT_FALLBACK });
    const pdfBuffer = await buildWorkOrderPdfBuffer({ activity: data, dealTitle, typeOfService, location, channelForQR: channelIdForQr || DEFAULT_CHANNEL, assigneeName });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="WO_${aid}.pdf"`);
    return res.send(pdfBuffer);
  }catch(e){
    console.error('/wo/pdf error', e);
    res.status(500).send('error');
  }
});

/* ========= Start ========= */
(async () => { await app.start(PORT); console.log(`✅ Dispatcher running on port ${PORT}`); })();
