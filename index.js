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
const BASE_URL = process.env.BASE_URL; // e.g. https://dispatcher-production-xxx.up.railway.app
const PD_WEBHOOK_KEY = process.env.PD_WEBHOOK_KEY; // append ?key=... in PD webhook
const ALLOW_DEFAULT_FALLBACK = process.env.ALLOW_DEFAULT_FALLBACK !== 'false';
const FORCE_CHANNEL_ID = process.env.FORCE_CHANNEL_ID || null;

// Feature toggles
const ENABLE_PD_FILE_UPLOAD   = process.env.ENABLE_PD_FILE_UPLOAD   !== 'false'; // attach PDF to PD deal
const ENABLE_PD_NOTE          = process.env.ENABLE_PD_NOTE          !== 'false'; // add PD note
const ENABLE_SLACK_PDF_UPLOAD = process.env.ENABLE_SLACK_PDF_UPLOAD !== 'false'; // upload PDF in Slack
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
const PRODUCTION_TEAM_MAP = { 47:'Kings',48:'Johnathan',49:'Pena',50:'Hector',51:'Sebastian',52:'Anastacio',53:'Mike',54:'Kim' };

// PD custom field key (Production Team on DEAL)
const PRODUCTION_TEAM_FIELD_KEY = '8bbab3c120ade3217b8738f001033064e803cdef';

// Production Team enum ID → Slack channel
const PRODUCTION_TEAM_TO_CHANNEL = {
  52: 'C09BA0XUAV7',   // Anastacio
  53: 'C098H8GU355',   // Mike  (daily tasks / fallback)
  // add more when ready
};

// Fallback name→channel for parsing "Crew: Name" in title/subject
const NAME_TO_CHANNEL = { anastacio:'C09BA0XUAV7', mike:'C098H8GU355' };
const NAME_TO_TEAM_ID = { anastacio:52, mike:53 };

/* ========= Slack App ========= */
const receiver = new ExpressReceiver({
  signingSecret: SLACK_SIGNING_SECRET,
  endpoints: { events:'/slack/events', commands:'/slack/commands', actions:'/slack/interact' },
  processBeforeResponse: true,
});
const app = new App({ token: SLACK_BOT_TOKEN, signingSecret: SLACK_SIGNING_SECRET, receiver });

app.event('app_mention', async ({ event, say }) => { await say(`Hey <@${event.user}>, Dispatcher is online and running!`); });

app.action('complete_task', async ({ body, ack, client }) => {
  await ack();
  const val = body.actions?.[0]?.selected_options?.[0]?.value;
  const activityId = val?.replace('task_', '');
  if (!activityId) return;
  try {
    await fetch(`https://api.pipedrive.com/v1/activities/${activityId}?api_token=${PIPEDRIVE_API_TOKEN}`, {
      method:'PUT', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ done:true, marked_as_done_time: new Date().toISOString() })
    });
  } catch (e) { console.error('PD complete error:', e); }
  try { await client.chat.delete({ channel: body.channel.id, ts: body.message.ts }); } catch(e){ console.error('Slack delete error:', e); }
});

/* ========= Express ========= */
const expressApp = receiver.app;
expressApp.use(express.json());
expressApp.get('/', (_req,res)=>res.status(200).send('Dispatcher OK'));
expressApp.get('/healthz', (_req,res)=>res.status(200).send('ok'));

/* ========= Helpers ========= */
const b64url = (b)=>Buffer.from(b).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
const sign = (raw)=>b64url(crypto.createHmac('sha256', SIGNING_SECRET).update(raw).digest());
function verify(raw,sig){ try{ const a=Buffer.from(sign(raw)), b=Buffer.from(String(sig)); if(a.length!==b.length) return false; return crypto.timingSafeEqual(a,b);}catch{return false;} }
const cleanBase = ()=> String(BASE_URL||'').trim().replace(/^=+/, '');

// Minimal HTML→text for PD notes (bullets, line breaks, entities)
function htmlToPlainText(input=''){
  let s = String(input);
  s = s.replace(/<br\s*\/?>/gi, '\n');
  s = s.replace(/<\/?p[^>]*>/gi, '\n');
  s = s.replace(/<\/li>\s*<li[^>]*>/gi, '\n• ');
  s = s.replace(/<li[^>]*>/gi, '• ');
  s = s.replace(/<\/li>/gi, '');
  s = s.replace(/<\/?ul[^>]*>/gi, '\n');
  s = s.replace(/<\/?ol[^>]*>/gi, '\n');
  s = s.replace(/<[^>]+>/g, '');
  s = s.replace(/&nbsp;/g, ' ')
       .replace(/&amp;/g, '&')
       .replace(/&lt;/g, '<')
       .replace(/&gt;/g, '>')
       .replace(/&quot;/g, '"')
       .replace(/&#39;/g, "'");
  s = s.replace(/\n{3,}/g, '\n\n').trim();
  return s;
}

// Signed URL for QR
function makeSignedCompleteUrl({ aid, did='', cid='', ttlSeconds=7*24*60*60 }){
  const exp = Math.floor(Date.now()/1000)+ttlSeconds;
  const raw = `${aid}.${did}.${cid}.${exp}`;
  const params = new URLSearchParams({ aid:String(aid), exp:String(exp), sig:sign(raw) });
  if (did) params.set('did', String(did));
  if (cid) params.set('cid', String(cid));
  return `${cleanBase()}/wo/complete?${params.toString()}`;
}

// Slack button URL
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

// PD time normalization
function getTimeField(v){ if(!v) return ''; if(typeof v==='string') return v; if(typeof v==='object' && v.value) return String(v.value); return ''; }

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
      if (exact || suffix || fuzzy){ channelCache.set(key, c.id); return c.id; }
    }
    if (!resp.response_metadata?.next_cursor) break;
    cursor = resp.response_metadata.next_cursor;
  }
  return null;
}

async function ensureBotInChannel(channelId){
  if (!channelId) return;
  try{ await app.client.conversations.join({ channel: channelId }); }
  catch(e){ /* ignore already_in_channel etc. */ }
}

async function resolveDealChannelId({ dealId, allowDefault = ALLOW_DEFAULT_FALLBACK }){
  const byDeal = await findDealChannelId(dealId);
  if (byDeal) return byDeal;
  return allowDefault ? DEFAULT_CHANNEL : null;
}

/* ========= Assignee detection (multi-source) ========= */
function detectAssignee({ deal, activity }){
  // Prefer Deal field
  if (deal){
    const tid = deal[PRODUCTION_TEAM_FIELD_KEY];
    if (tid && PRODUCTION_TEAM_TO_CHANNEL[tid]) {
      return { teamId: tid, teamName: PRODUCTION_TEAM_MAP[tid] || `Team ${tid}`, channelId: PRODUCTION_TEAM_TO_CHANNEL[tid] };
    }
  }
  // Fallback parse "Crew: Name" in title or subject
  const crewFrom = (s) => (s ? (String(s).match(/Crew:\s*([A-Za-z]+)/i)?.[1] || null) : null);
  const name = crewFrom(deal?.title) || crewFrom(activity?.subject);
  if (name){
    const key = name.toLowerCase();
    const channelId = NAME_TO_CHANNEL[key] || null;
    const teamId = NAME_TO_TEAM_ID[key] || null;
    const teamName = PRODUCTION_TEAM_MAP[teamId] || (name.charAt(0).toUpperCase()+name.slice(1));
    if (channelId) return { teamId, teamName, channelId };
  }
  return { teamId:null, teamName:null, channelId:null };
}

/* ========= Reassignment & completion tracking =========
   activityId -> { assigneeChannelId, messageTs, fileIds: string[] }
   NOTE: in-memory (resets on restart); good enough for now.
*/
const ASSIGNEE_POSTS = new Map();

/* ========= PDF builder ========= */
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
  doc.font('Helvetica').fontSize(10).fillColor('#555').text('Scanning marks this task complete in Pipedrive and posts a confirmation in Slack.');
  doc.moveDown(0.5);
  doc.image(qrBuffer, { fit:[120,120] });
  doc.moveDown(0.25);
  doc.fontSize(8).fillColor('#777').text(completeUrl, { width: 520 });

  doc.end();
  return done;
}

/* ========= Slack helpers ========= */
function buildSlackBlocks({ activity, noteText, dealId, dealTitle, typeOfService, location, assigneeName }){
  const scheduledText = [activity.due_date, getTimeField(activity.due_time)].filter(Boolean).join(' ') || 'No due date';
  const pdfUrl = buildPdfUrl(activity.id);
  const actionElements = [{
    type:'checkboxes', action_id:'complete_task',
    options:[{ text:{type:'mrkdwn', text:'Mark as complete'}, value:`task_${activity.id}` }]
  }];
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
      (assigneeName ? `\n👷 Assigned To: ${assigneeName}` : '')
    }},
    { type:'actions', elements: actionElements },
  ];
}

async function uploadPdfToSlack({ channel, filename, pdfBuffer, title, initialComment='' }){
  return app.client.files.uploadV2({ channel_id: channel, file: pdfBuffer, filename, title: title||filename, initial_comment: initialComment });
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

/* ========= Delete helpers (message + files) ========= */
async function deleteAssigneePost(activityId){
  const key = String(activityId);
  const rec = ASSIGNEE_POSTS.get(key);
  if (!rec) return;
  const { assigneeChannelId, messageTs, fileIds } = rec;

  if (assigneeChannelId && messageTs){
    try { await app.client.chat.delete({ channel: assigneeChannelId, ts: messageTs }); }
    catch (e){ console.warn('[WO] chat.delete failed (assignee):', e?.data || e?.message || e); }
  }
  if (Array.isArray(fileIds)){
    for (const fid of fileIds){
      if (!fid) continue;
      try { await app.client.files.delete({ file: fid }); }
      catch (e){ console.warn('[WO] files.delete failed:', fid, e?.data || e?.message || e); }
    }
  }
  ASSIGNEE_POSTS.delete(key);
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

  // Job channel (persistent)
  if (jobChannelId){
    await ensureBotInChannel(jobChannelId);
    await app.client.chat.postMessage({
      channel: jobChannelId,
      text: `📌 New Task • ${activity.subject || '-'} • Due: ${scheduledText}`,
      blocks
    });
    if (ENABLE_SLACK_PDF_UPLOAD){
      try { await uploadPdfToSlack({ channel: jobChannelId, filename, pdfBuffer, title:`Work Order — ${activity.subject || ''}`, initialComment:'📄 Work Order PDF (scan QR to complete)' }); }
      catch(e){ console.warn('[WO] files.uploadV2 (job) failed:', e?.data || e?.message || e); }
    }
  }

  // Assignee channel (delete-on-reassign + file tracking)
  if (assigneeChannelId){
    await ensureBotInChannel(assigneeChannelId);

    // If we are re-posting for the same activity, clear previous assignee post/files first.
    await deleteAssigneePost(activity.id);

    const aMsg = await app.client.chat.postMessage({
      channel: assigneeChannelId,
      text: `📌 New Task • ${activity.subject || '-'} • Due: ${scheduledText}`,
      blocks
    });

    const record = { assigneeChannelId, messageTs: aMsg.ts, fileIds: [] };

    if (ENABLE_SLACK_PDF_UPLOAD){
      try {
        const up = await uploadPdfToSlack({
          channel: assigneeChannelId,
          filename,
