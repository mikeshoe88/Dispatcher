const jobChannel = cid || await resolveDealChannelId({ dealId: dealIdForUpload, allowDefault: ALLOW_DEFAULT_FALLBACK }) || DEFAULT_CHANNEL;
      if (jobChannel) {
        // Always post completion PDF to job channel
        await uploadPdfToSlack({ channel: jobChannel, filename: `WO_${aid}_Completed.pdf`, pdfBuffer: completedPdf, title: 'Work Order Completed', initialComment: `✅ Completed Work Order for activity ${aid}. ${AID_TAG(aid)}` });
      }
    } catch// index.js (ESM)
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
const ENABLE_PD_NOTE            = false; // only attach WO PDF, not initial PD note
const ENABLE_SLACK_PDF_UPLOAD   = process.env.ENABLE_SLACK_PDF_UPLOAD   !== 'false';
const ENABLE_DELETE_ON_REASSIGN = process.env.ENABLE_DELETE_ON_REASSIGN !== 'false';

// Job-channel posting behavior: 'immediate' | 'on_assigned' | 'on_complete'
// on_assigned = only post to job channel when a Production Team is set
const JOB_CHANNEL_MODE = (process.env.JOB_CHANNEL_MODE || 'on_assigned').toLowerCase();
const JOB_CHANNEL_MODES = new Set(['immediate','on_assigned','on_complete']);
function shouldPostToJobChannel({ assigneeChannelId }) {
  const mode = JOB_CHANNEL_MODES.has(JOB_CHANNEL_MODE) ? JOB_CHANNEL_MODE : 'on_assigned';
  if (mode === 'immediate') return true;
  if (mode === 'on_assigned') return !!assigneeChannelId; // only once assigned
  if (mode === 'on_complete') return false; // never post until QR completion
  return true;
}

// Job-channel content style: 'pdf' | 'summary'
// summary = post a light text notice only (no PDF/notes) until completion
const JOB_CHANNEL_STYLE = (process.env.JOB_CHANNEL_STYLE || 'summary').toLowerCase();

// Job-channel posting behavior: 'immediate' | 'on_assigned' | 'on_complete'
// on_assigned = only post to job channel when a Production Team is set
const JOB_CHANNEL_MODE = (process.env.JOB_CHANNEL_MODE || 'on_assigned').toLowerCase();
const JOB_CHANNEL_MODES = new Set(['immediate','on_assigned','on_complete']);
function shouldPostToJobChannel({ assigneeChannelId }) {
  const mode = JOB_CHANNEL_MODES.has(JOB_CHANNEL_MODE) ? JOB_CHANNEL_MODE : 'on_assigned';
  if (mode === 'immediate') return true;
  if (mode === 'on_assigned') return !!assigneeChannelId; // only once assigned
  if (mode === 'on_complete') return false; // never post until QR completion
  return true;
}

// Optional: skip invoice/billing tasks entirely
const SKIP_INVOICE_TASKS = process.env.SKIP_INVOICE_TASKS !== 'false';
const INVOICE_KEYWORDS = (process.env.INVOICE_KEYWORDS || 'invoice,billing,billed,bill,payment request,collect payment,final invoice,send invoice')
  .split(',')
  .map(s => s.trim().toLowerCase())
  .filter(Boolean);

if (!SLACK_SIGNING_SECRET) throw new Error('Missing SLACK_SIGNING_SECRET');
if (!SLACK_BOT_TOKEN) throw new Error('Missing SLACK_BOT_TOKEN');
if (!PIPEDRIVE_API_TOKEN) throw new Error('Missing PIPEDRIVE_API_TOKEN');
if (!SIGNING_SECRET) throw new Error('Missing WO_QR_SECRET');
if (!BASE_URL) throw new Error('Missing BASE_URL');
if (!PD_WEBHOOK_KEY) console.warn('⚠️ PD_WEBHOOK_KEY not set; /pipedrive-task will 403.');

// Crash handlers
process.on('unhandledRejection', (r)=>console.error('[FATAL] Unhandled Rejection:', r?.stack||r));
process.on('uncaughtException', (e)=>console.error('[FATAL] Uncaught Exception:', e?.stack||e));

/* ========= Anti-spam guards ========= */
const MAX_ACTIVITY_AGE_DAYS = Number(process.env.MAX_ACTIVITY_AGE_DAYS || 14); // ignore very old activities by default
const POSTED_CACHE = new Map(); // bursts guard
const POST_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes per version

function alreadyHandled(activity){
  const version = String(activity.update_time || activity.add_time || '');
  const key = `aid:${activity.id}|v:${version}`;
  const now = Date.now();
  const exp = POSTED_CACHE.get(key);
  if (exp && exp > now) return true;
  if (POSTED_CACHE.size > 500){ for (const [k,t] of POSTED_CACHE){ if (t<=now) POSTED_CACHE.delete(k); } }
  POSTED_CACHE.set(key, now + POST_CACHE_TTL_MS);
  return false;
}

function isActivityFresh(activity, maxDays = MAX_ACTIVITY_AGE_DAYS){
  if (!maxDays) return true;
  const now = Date.now();
  const due = activity?.due_date ? new Date(activity.due_date) : null;
  const add = activity?.add_time ? new Date(String(activity.add_time).replace(' ', 'T')) : null;
  const ref = due || add;
  if (!ref || isNaN(ref)) return true; // allow if unclear
  return (now - ref.getTime()) <= maxDays * 86400000;
}

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
  anastacio:'C09BA0XUAV7', mike:'C098H8GU355', greg:'C09BFFGBYTB', amber:'C09B49MJHEE',
  'anna marie':'C09B85LE544', annamarie:'C09B85LE544', kings:'C09BXCCD95W', penabad:'C09ASBE36Q7',
  johnathan:'C09ASB1N32B', gary:'C09AZ63JEJF', hector:'C09B6P5LVPY', sebastian:'C09AZ6VT459'
};
const NAME_TO_TEAM_ID = { anastacio:52, mike:53, greg:55, amber:56, 'anna marie':57, annamarie:57, kings:47, penabad:49, johnathan:48, gary:54, hector:50, sebastian:51 };

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

// Only treat OPEN deals as eligible for WO posts
function isDealActive(deal){
  if (!deal) return true; // unknown -> allow
  const status = String(deal.status || '').toLowerCase();
  if (status && status !== 'open') return false; // won/lost/deleted
  if (deal.active_flag === false) return false;  // archived
  return true;
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

// Slack file upload helper
async function uploadPdfToSlack({ channel, filename, pdfBuffer, title, initialComment='' }){
  return app.client.files.uploadV2({
    channel_id: channel,
    file: pdfBuffer,
    filename,
    title: title||filename,
    initial_comment: initialComment
  });
}

// PD upload helper
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

// PD note (used on completion only)
async function postPdNote({ dealId, content }){
  if(!dealId) return;
  try{
    const resp = await fetch(`https://api.pipedrive.com/v1/notes?api_token=${PIPEDRIVE_API_TOKEN}`,{
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ deal_id: dealId, content })
    });
    return await resp.json();
  }catch(e){ console.error('[WO] PD note failed', e?.message||e); }
}

// PD time normalization
function getTimeField(v){ if(!v) return ''; if(typeof v==='string') return v; if(typeof v==='object' && v.value) return String(v.value); return ''; }

// ====== Strict invoice/billing detector (prevents rename/posting) ======
function isInvoiceLike(activity){
  if (!SKIP_INVOICE_TASKS) return false;

  const subjectRaw = (activity?.subject || '').toLowerCase().trim();
  const subject = subjectRaw.replace(/\s+/g, ' ');
  const note = htmlToPlainText(activity?.note || '').toLowerCase();

  const EXACTS = (process.env.INVOICE_EXACT_SUBJECTS || 'billed/invoice,invoice,invoice task,collect payment,final invoice,bill in 5 days')
    .split(',').map(s => s.trim().toLowerCase()).filter(Boolean);

  if (EXACTS.includes(subject)) return true;
  if (subject.startsWith('invoice:') || subject.startsWith('invoice -') || subject.startsWith('billed/')) return true;

  const escaped = INVOICE_KEYWORDS.map(k => k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  const re = new RegExp(`\\b(${escaped.join('|')})\\b`, 'i');
  if (re.test(subject) || re.test(note)) return true;

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

/* ========= Assignee detection ========= */
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
  // 3) Fallback parse from title/subject
  const crewFrom = (s) => (s ? (String(s).match(/Crew:\s*([A-Za-z][A-Za-z ]*)/i)?.[1] || null) : null);
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

/* ========= Reassignment & completion tracking ========= */
const ASSIGNEE_POSTS = new Map(); // activityId -> { assigneeChannelId, messageTs, fileIds: string[] }
const AID_TAG = (id)=>`[AID:${id}]`;

/* ========= PDF builders ========= */
async function buildWorkOrderPdfBuffer({ activity, dealTitle, typeOfService, location, channelForQR, assigneeName, customerName, jobNumber }) {
  const completeUrl = makeSignedCompleteUrl({
    aid: String(activity.id),
    did: activity.deal_id ? String(activity.deal_id) : '',
    cid: channelForQR || DEFAULT_CHANNEL,
  });

  const qrDataUrl = await QRCode.toDataURL(completeUrl);
  const qrBuffer = Buffer.from(qrDataUrl.replace(/^data:image\/png;base64\,/,'') ,'base64');

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
  doc.text(`Job Number: ${jobNumber || activity.deal_id || '-'}`);
  if (customerName) doc.text(`Customer: ${customerName}`);
  doc.text(`Type of Service: ${typeOfService || '-'}`);
  doc.text(`Address: ${location || '-'}`);
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
    } else if (assigneeChannelId) {
      await deleteAssigneePdfByMarker(activityId, assigneeChannelId, 400);
    }
    if (Array.isArray(fileIds)){
      for (const fid of fileIds){ if (!fid) continue; try { await app.client.files.delete({ file: fid }); } catch(e){} }
    }
    ASSIGNEE_POSTS.delete(key);
  }
}

/* ========= Message builder ========= */
function buildSummary({ activity, deal, assigneeName, noteText }){
  const dealId = activity.deal_id || (deal?.id ?? 'N/A');
  const dealTitle = deal?.title || 'N/A';
  const serviceId = deal ? deal['5b436b45b63857305f9691910b6567351b5517bc'] : null;
  const typeOfService = SERVICE_MAP[serviceId] || serviceId || 'N/A';
  return [
    `📌 Work Order • [JOB ${deal?.id || 'N/A'}] ${dealTitle} — ${activity.subject || ''} — Crew: ${assigneeName || 'Unassigned'}`,
    `🏷️ Deal: ${dealId} — ${dealTitle}`,
    `📦 ${typeOfService}`,
    assigneeName ? `👷 ${assigneeName}` : null,
    noteText || activity.note ? `
📜 Notes:
${htmlToPlainText(noteText || activity.note)}` : null,
    `
Scan the QR in the PDF to complete. ${AID_TAG(activity.id)}`
  ].filter(Boolean).join('
');
}

function buildJobChannelNotice({ activity, deal, assigneeName }){
  const dealTitle = deal?.title || 'N/A';
  const subj = activity?.subject || 'Task';
  const who = assigneeName || 'Unassigned';
  return [
    `🧭 *${subj}* has been assigned to *${who}*. ${AID_TAG(activity.id)}`,
    `📨 Work order delivered to ${who === 'Unassigned' ? 'assignee channel' : `*${who}*'s channel`}.`
  ].join('
');
}

/* ========= Core post ========= */
async function postWorkOrderToChannels({ activity, deal, jobChannelId, assigneeChannelId, assigneeName, noteText }){
  const dealId = activity.deal_id || (deal?.id ?? 'N/A');
  const dealTitle = deal?.title || 'N/A';
  const serviceId = deal ? deal['5b436b45b63857305f9691910b6567351b5517bc'] : null;
  const typeOfService = SERVICE_MAP[serviceId] || serviceId || 'N/A';
  const location = deal?.location || 'N/A';

  // fetch customer name (best-effort)
  let customerName = null;
  try {
    const pid = deal?.person_id?.value || deal?.person_id?.id || deal?.person_id;
    if (pid) {
      const pres = await fetch(`https://api.pipedrive.com/v1/persons/${encodeURIComponent(pid)}?api_token=${PIPEDRIVE_API_TOKEN}`);
      const pjson = await pres.json();
      if (pjson?.success && pjson.data) customerName = pjson.data.name || null;
    }
  } catch {}

  const pdfBuffer = await buildWorkOrderPdfBuffer({
    activity,
    dealTitle,
    typeOfService,
    location,
    channelForQR: jobChannelId || DEFAULT_CHANNEL,
    assigneeName,
    customerName,
    jobNumber: deal?.id
  });

  const safe = (s) => (s || '').toString().replace(/[^\w\-]+/g,'_').slice(0,60);
  const filename = `WO_${safe(dealTitle)}_${safe(activity.subject)}.pdf`;
  const summary = buildSummary({ activity, deal, assigneeName, noteText });

  // Job channel (respect mode + active deal)
  if (jobChannelId && isDealActive(deal) && shouldPostToJobChannel({ assigneeChannelId })){
    await ensureBotInChannel(jobChannelId);
    try {
      if (JOB_CHANNEL_STYLE === 'summary') {
        const notice = buildJobChannelNotice({ activity, deal, assigneeName });
        await app.client.chat.postMessage({ channel: jobChannelId, text: notice });
      } else if (ENABLE_SLACK_PDF_UPLOAD) {
        await uploadPdfToSlack({ channel: jobChannelId, filename, pdfBuffer, title:`Work Order — ${activity.subject || ''}`, initialComment: summary });
      }
    } catch(e){ console.warn('[WO] job channel post failed:', e?.data || e?.message || e); }
  }

  // Assignee channel (personal) — always post for their workflow
  if (assigneeChannelId){
    await ensureBotInChannel(assigneeChannelId);

    if (ENABLE_DELETE_ON_REASSIGN) { await deleteAssigneePost(activity.id); }

    if (ENABLE_SLACK_PDF_UPLOAD){
      try {
        const up = await uploadPdfToSlack({ channel: assigneeChannelId, filename, pdfBuffer, title:`Work Order — ${activity.subject || ''}`, initialComment: summary });
        const fids = [];
        if (up?.files && Array.isArray(up.files)) { for (const f of up.files) if (f?.id) fids.push(f.id); }
        else if (up?.file?.id) { fids.push(up.file.id); }
        const aMsgTs = up?.file?.shares?.public?.[assigneeChannelId]?.[0]?.ts || up?.file?.shares?.private?.[assigneeChannelId]?.[0]?.ts || null;
        ASSIGNEE_POSTS.set(String(activity.id), { assigneeChannelId, messageTs: aMsgTs, fileIds: fids });
      } catch(e){ console.warn('[WO] files.uploadV2 (assignee) failed:', e?.data || e?.message || e); }
    }
  }

  if (ENABLE_PD_FILE_UPLOAD){ try{ await uploadPdfToPipedrive({ dealId, pdfBuffer, filename }); } catch(e){ console.error('[WO] PD upload failed:', e); } }
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

    if (entity === 'activity' && (action === 'create' || action === 'update')) {
      if (!data?.id) return res.status(200).send('No activity');

      // ignore invoice-like & done
      if (isInvoiceLike(data)) { console.log('[PD Hook] skip id=%s (invoice-like, pre)', data.id); return res.status(200).send('OK'); }
      if (data.done === true || data.done === 1) { console.log('[PD Hook] skip id=%s (already done)', data.id); return res.status(200).send('OK'); }

      // Fetch full activity
      const aRes = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(data.id)}?api_token=${PIPEDRIVE_API_TOKEN}`);
      const aJson = await aRes.json();
      if (!aJson?.success || !aJson.data) return res.status(200).send('Activity fetch fail');
      const activity = aJson.data;

      if (alreadyHandled(activity)) { console.log('[PD Hook] skip id=%s (burst duplicate)', activity.id); return res.status(200).send('OK'); }
      if (!isActivityFresh(activity)) { console.log('[PD Hook] skip id=%s (stale)', activity.id); return res.status(200).send('OK'); }
      if (isInvoiceLike(activity)) { console.log('[PD Hook] skip id=%s (invoice-like, post)', activity.id); return res.status(200).send('OK'); }
      if (activity.done === true || activity.done === 1) { console.log('[PD Hook] skip id=%s (done after fetch)', activity.id); return res.status(200).send('OK'); }

      // Fetch deal
      let deal = null;
      if (activity.deal_id) {
        const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(activity.deal_id)}?api_token=${PIPEDRIVE_API_TOKEN}`);
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) deal = dJson.data;
        if (!isDealActive(deal)) { console.log('[PD Hook] skip id=%s (deal closed/deleted)', activity.id); return res.status(200).send('OK'); }
      }

      // On update: clean old posts if team changed
      if (action === 'update') {
        const prevTeamId = prev ? prev[PRODUCTION_TEAM_FIELD_KEY] : undefined;
        const prevCrew = (s)=> (s ? (String(s).match(/Crew:\s*([A-Za-z][A-Za-z ]*)/i)?.[1] || null) : null);
        const prevCrewName = prevCrew(prev?.subject);

        const newAss = detectAssignee({ deal, activity });
        const oldChannel = (()=>{
          if (prevTeamId && PRODUCTION_TEAM_TO_CHANNEL[prevTeamId]) return PRODUCTION_TEAM_TO_CHANNEL[prevTeamId];
          if (prevCrewName) { const key = prevCrewName.toLowerCase(); return NAME_TO_CHANNEL[key] || null; }
          return null;
        })();

        const changed =
          (prevTeamId !== undefined && prevTeamId !== activity[PRODUCTION_TEAM_FIELD_KEY]) ||
          (prevCrewName && prevCrewName.toLowerCase() !== (newAss.teamName||'').toLowerCase());

        if (changed && ENABLE_DELETE_ON_REASSIGN) {
          if (oldChannel) { console.log('[WO] update → delete old assignee post in %s', oldChannel); await deleteAssigneePdfByMarker(activity.id, oldChannel, 400); }
          await deleteAssigneePost(activity.id);
          const oldJobChannel = await resolveDealChannelId({ dealId: activity.deal_id, allowDefault: ALLOW_DEFAULT_FALLBACK });
          if (oldJobChannel) await deleteAssigneePdfByMarker(activity.id, oldJobChannel, 400);
        }
      }

      let jobChannelId = await resolveDealChannelId({ dealId: activity.deal_id, allowDefault: ALLOW_DEFAULT_FALLBACK });
      if (FORCE_CHANNEL_ID) jobChannelId = FORCE_CHANNEL_ID;
      const assignee = detectAssignee({ deal, activity });

      console.log('[PD Hook] ACTIVITY %s → deal=%s assigneeChannel=%s assignee=%s', action, activity.deal_id || 'N/A', assignee.channelId || 'none', assignee.teamName || 'unknown');

      await postWorkOrderToChannels({ activity, deal, jobChannelId, assigneeChannelId: assignee.channelId, assigneeName: assignee.teamName, noteText: activity.note });
      return res.status(200).send('OK');
    }

    if (entity === 'deal' && action === 'update') {
      const deal = data;
      if (!deal?.id) return res.status(200).send('No deal');

      const oldTeamId = prev ? prev[PRODUCTION_TEAM_FIELD_KEY] : undefined;
      const newTeamId = deal[PRODUCTION_TEAM_FIELD_KEY];
      const crewNameFrom = (s)=> (s ? (String(s).match(/Crew:\s*([A-Za-z][A-Za-z ]*)/i)?.[1] || null) : null);
      const oldCrewName = crewNameFrom(prev?.title);
      const newCrewName = crewNameFrom(deal?.title);

      const teamChanged = oldTeamId !== newTeamId;
      const crewChanged = (oldCrewName || newCrewName) && (oldCrewName !== newCrewName);
      console.log('[PD Hook] DEAL update teamChanged=%s crewChanged=%s oldTeam=%s newTeam=%s oldCrew=%s newCrew=%s', teamChanged, crewChanged, oldTeamId, newTeamId, oldCrewName, newCrewName);
      if (!teamChanged && !crewChanged) return res.status(200).send('OK');

      if (!isDealActive(deal)) return res.status(200).send('OK');

      const assignee = detectAssignee({ deal, activity: null });
      if (!assignee.channelId) { console.log('[PD Hook] no assignee channel for deal update'); return res.status(200).send('OK'); }

      let jobChannelId = await resolveDealChannelId({ dealId: deal.id, allowDefault: ALLOW_DEFAULT_FALLBACK });
      if (FORCE_CHANNEL_ID) jobChannelId = FORCE_CHANNEL_ID;

      const listRes = await fetch(`https://api.pipedrive.com/v1/activities?deal_id=${encodeURIComponent(deal.id)}&done=0&start=0&limit=50&api_token=${PIPEDRIVE_API_TOKEN}`);
      const listJson = await listRes.json();
      const items = (listJson?.data || []).filter(a => a && (a.done === false || a.done === 0) && !isInvoiceLike(a) && isActivityFresh(a));
      console.log('[PD Hook] DEAL update → repost %d open activities', items.length);

      const oldChannel = (()=>{
        if (oldTeamId && PRODUCTION_TEAM_TO_CHANNEL[oldTeamId]) return PRODUCTION_TEAM_TO_CHANNEL[oldTeamId];
        if (oldCrewName) { const key = oldCrewName.toLowerCase(); return NAME_TO_CHANNEL[key] || null; }
        return null;
      })();

      for (const activity of items) {
        try {
          if (ENABLE_DELETE_ON_REASSIGN) {
            if (oldChannel) await deleteAssigneePdfByMarker(activity.id, oldChannel, 200);
            await deleteAssigneePost(activity.id);
            const oldJobChannel = await resolveDealChannelId({ dealId: deal.id, allowDefault: ALLOW_DEFAULT_FALLBACK });
            if (oldJobChannel) await deleteAssigneePdfByMarker(activity.id, oldJobChannel, 400);
          }
          await postWorkOrderToChannels({ activity, deal, jobChannelId, assigneeChannelId: assignee.channelId, assigneeName: assignee.teamName, noteText: activity.note });
        } catch (e) { console.error('[WO] re-route failed', activity?.id, e?.message||e); }
      }

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

    // 1) Mark PD activity done
    const markedAtIso = new Date().toISOString();
    const pdResp = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(aid)}?api_token=${PIPEDRIVE_API_TOKEN}`, {
      method:'PUT', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ done:true, marked_as_done_time: markedAtIso })
    });
    const pdJson = await pdResp.json();
    const ok = !!(pdJson && pdJson.success);

    // 2) Fetch activity + deal (for PDF fields)
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

    const dealIdForUpload = did || activity?.deal_id;

    // 3) Generate and upload a *Completed Work Order* PDF to PD and Slack
    try {
      const completedPdf = await buildCompletionPdfBuffer({ activity, dealTitle, typeOfService, location, completedAt: Date.now() });
      const up = await uploadPdfToPipedrive({ dealId: dealIdForUpload, pdfBuffer: completedPdf, filename: `WO_${aid}_Completed.pdf` });
      console.log('[WO] completion PDF uploaded', up?.success);

      // Job channel gets the completion, even if JOB_CHANNEL_MODE was 'on_complete'
      const jobChannel = cid || await resolveDealChannelId({ dealId: dealIdForUpload, allowDefault: ALLOW_DEFAULT_FALLBACK }) || DEFAULT_CHANNEL;
      if (jobChannel) {
        await uploadPdfToSlack({ channel: jobChannel, filename: `WO_${aid}_Completed.pdf`, pdfBuffer: completedPdf, title: 'Work Order Completed', initialComment: `✅ Completed Work Order for activity ${aid}. ${AID_TAG(aid)}` });
      }
    } catch (e) {
      console.error('[WO] completion PDF upload failed', e?.message||e);
    }

    // 3.5) Add a Pipedrive note announcing completion
    try {
      const when = new Date().toLocaleString();
      const subject = activity?.subject ? `“${activity.subject}”` : '';
      await postPdNote({
        dealId: dealIdForUpload,
        content: `✅ Work Order Completed via QR Scan.\nActivity ${aid} ${subject} marked done at ${when}.\nA completion PDF has been attached to the deal.`
      });
    } catch(e) { console.error('[WO] PD completion note failed', e?.message||e); }

    // 4) Clean up assignee channel WO message/files (hide from personal channel)
    await deleteAssigneePost(aid);

    res.status(200).send(
      `<html><body style="font-family:Arial;padding:24px"><h2>Work Order Complete</h2>
       <p>Task <b>${aid}</b> ${ok ? 'has been updated' : 'could not be updated'} in Pipedrive.</p>
       ${did ? `<p>Deal: <b>${did}</b></p>` : ''}
       <p>${ok ? 'A completion PDF and note have been attached. ✅' : 'Please contact the office. ⚠️'}</p></body></html>`
    );
  } catch (err) {
    console.error('/wo/complete error:', err);
    res.status(500).send('Server error.');
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

    if (isInvoiceLike(data)) return res.status(403).send('Forbidden for invoice activities');
    if (!isActivityFresh(data)) return res.status(403).send('Forbidden for stale activities');

    let dealTitle='N/A', typeOfService='N/A', location='N/A', assigneeName=null, deal=null, customerName=null;
    const dealId = data.deal_id;
    if (dealId){
      const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(dealId)}?api_token=${PIPEDRIVE_API_TOKEN}`);
      const dj = await dRes.json();
      if (dj?.success && dj.data){
        deal = dj.data;
        if (!isDealActive(deal)) return res.status(403).send('Deal not active');
        dealTitle = deal.title || 'N/A';
        const serviceId = deal['5b436b45b63857305f9691910b6567351b5517bc'];
        typeOfService = SERVICE_MAP[serviceId] || serviceId || 'N/A';
        location = deal.location || 'N/A';
        const ass = detectAssignee({ deal, activity: data });
        assigneeName = ass.teamName || assigneeName;
        const pid = deal?.person_id?.value || deal?.person_id?.id || deal?.person_id;
        if (pid) {
          const pres = await fetch(`https://api.pipedrive.com/v1/persons/${encodeURIComponent(pid)}?api_token=${PIPEDRIVE_API_TOKEN}`);
          const pjson = await pres.json();
          if (pjson?.success && pjson.data) customerName = pjson.data.name || null;
        }
      }
    }

    const channelIdForQr = await resolveDealChannelId({ dealId, allowDefault: ALLOW_DEFAULT_FALLBACK });
    const pdfBuffer = await buildWorkOrderPdfBuffer({ activity: data, dealTitle, typeOfService, location, channelForQR: channelIdForQr || DEFAULT_CHANNEL, assigneeName, customerName, jobNumber: deal?.id });
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
