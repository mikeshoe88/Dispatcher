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
import { DateTime } from 'luxon';

/* ========= ENV / CONSTANTS ========= */
const PORT = process.env.PORT || 3000;
const SLACK_SIGNING_SECRET = process.env.SLACK_SIGNING_SECRET;
const SLACK_BOT_TOKEN      = process.env.SLACK_BOT_TOKEN;
const PIPEDRIVE_API_TOKEN  = process.env.PIPEDRIVE_API_TOKEN;
const SIGNING_SECRET       = process.env.WO_QR_SECRET;
const DEFAULT_CHANNEL      = process.env.DEFAULT_SLACK_CHANNEL_ID || 'C098H8GU355';
const BASE_URL             = process.env.BASE_URL;
const PD_WEBHOOK_KEY       = process.env.PD_WEBHOOK_KEY;
const TZ = 'America/Chicago';

/** Assignee resolution behavior */
const ALLOW_DEAL_FALLBACK  = (process.env.ALLOW_DEAL_FALLBACK || 'false').toLowerCase() === 'true';

/** Feature toggles kept minimal */
const ENABLE_PD_FILE_UPLOAD     = process.env.ENABLE_PD_FILE_UPLOAD     !== 'false';
const ENABLE_SLACK_PDF_UPLOAD   = process.env.ENABLE_SLACK_PDF_UPLOAD   !== 'false';
const ENABLE_DELETE_ON_REASSIGN = process.env.ENABLE_DELETE_ON_REASSIGN !== 'false';
const POST_FUTURE_WOS           = process.env.POST_FUTURE_WOS === 'true';

/** Invoice-skip */
const SKIP_INVOICE_TASKS = process.env.SKIP_INVOICE_TASKS !== 'false';
const INVOICE_KEYWORDS = (process.env.INVOICE_KEYWORDS || 'invoice,billing,billed,bill,payment request,collect payment,final invoice,send invoice,ar follow up,accounts receivable')
  .split(',').map(s => s.trim().toLowerCase()).filter(Boolean);

/* ===== Required envs ===== */
if (!SLACK_SIGNING_SECRET) throw new Error('Missing SLACK_SIGNING_SECRET');
if (!SLACK_BOT_TOKEN)      throw new Error('Missing SLACK_BOT_TOKEN');
if (!PIPEDRIVE_API_TOKEN)  throw new Error('Missing PIPEDRIVE_API_TOKEN');
if (!SIGNING_SECRET)       throw new Error('Missing WO_QR_SECRET');
if (!BASE_URL)             throw new Error('Missing BASE_URL');
if (!PD_WEBHOOK_KEY) console.warn('⚠️ PD_WEBHOOK_KEY not set; /pipedrive-task will 403.');

/* ========= Crash handlers ========= */
process.on('unhandledRejection', (r)=>console.error('[FATAL] Unhandled Rejection:', r?.stack||r));
process.on('uncaughtException', (e)=>console.error('[FATAL] Uncaught Exception:', e?.stack||e));

/* ========= Dedup / freshness ========= */
const MAX_ACTIVITY_AGE_DAYS = Number(process.env.MAX_ACTIVITY_AGE_DAYS || 14);
const EVENT_CACHE = new Map();
const EVENT_CACHE_TTL_MS = 60 * 1000;

function makeEventKey(meta = {}, activity = {}) {
  const id   = activity?.id || meta?.id || meta?.entity_id || 'n/a';
  const ts   = meta?.timestamp || meta?.time || '';
  const req  = meta?.request_id || meta?.requestId || '';
  const upd  = activity?.update_time || '';
  const done = activity?.done ? '1' : '0';
  const bucket = Math.floor(Date.now() / 10_000);
  return `aid:${id}|ts:${ts}|req:${req}|upd:${upd}|done:${done}|b:${bucket}`;
}
function alreadyHandledEvent(meta, activity) {
  const key = makeEventKey(meta, activity);
  const now = Date.now();
  const exp = EVENT_CACHE.get(key);
  if (exp && exp > now) return true;
  if (EVENT_CACHE.size > 5000) {
    for (const [k, t] of EVENT_CACHE) if (t <= now) EVENT_CACHE.delete(k);
  }
  EVENT_CACHE.set(key, now + EVENT_CACHE_TTL_MS);
  return false;
}

const POST_FINGERPRINT = new Map(); // aid -> { fp, exp }
const POST_FP_TTL_MS   = Number(process.env.POST_FP_TTL_MS || 10 * 60 * 1000);
function makePostFingerprint({ activity, assigneeName, deal }) {
  const due    = `${activity?.due_date || ''} ${activity?.due_time || ''}`;
  const subj   = String(activity?.subject || '');
  const who    = String(assigneeName || '');
  const dealId = String(deal?.id || activity?.deal_id || '');
  return `${subj}||${due}||${who}||${dealId}`;
}
function shouldPostNowStrong(activity, assigneeName, deal) {
  const id = String(activity?.id || '');
  if (!id) return false;
  const fp = makePostFingerprint({ activity, assigneeName, deal });
  const now = Date.now();
  for (const [k, v] of POST_FINGERPRINT) if (!v || v.exp <= now) POST_FINGERPRINT.delete(k);
  const prev = POST_FINGERPRINT.get(id);
  if (prev && prev.fp === fp) return false;
  POST_FINGERPRINT.set(id, { fp, exp: now + POST_FP_TTL_MS });
  return true;
}

function isActivityFresh(activity, maxDays = MAX_ACTIVITY_AGE_DAYS){
  if (!maxDays) return true;
  const now = Date.now();
  const due = activity?.due_date ? new Date(activity.due_date) : null;
  const add = activity?.add_time ? new Date(String(activity.add_time).replace(' ', 'T')) : null;
  const ref = due || add;
  if (!ref || isNaN(ref)) return true;
  return (now - ref.getTime()) <= maxDays * 86400000;
}
function isDealActive(deal){
  if (!deal) return true;
  const status = String(deal.status || '').toLowerCase();
  if (status && status !== 'open') return false;
  if (deal.active_flag === false) return false;
  return true;
}

/* ========= Date helpers ========= */
const isDueTodayCT = (activity) => {
  const d = (activity?.due_date||'').trim();
  if (!d) return false;
  const today = DateTime.now().setZone(TZ).toISODate();
  return d === today;
};
function _normalizeTime(val){
  if (val == null) return '';
  if (typeof val === 'string') return val.trim();
  if (typeof val === 'object' && val.value != null) return String(val.value).trim();
  if (typeof val === 'number') {
    const secs = val > 300 ? val : val * 60;
    const h = Math.floor(secs / 3600);
    const m = Math.floor((secs % 3600) / 60);
    const s = Math.floor(secs % 60);
    return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
  }
  return String(val).trim();
}
function parseDueDateTimeCT(activity){
  const d = String(activity?.due_date || '').trim();
  const tRaw = _normalizeTime(activity?.due_time);
  if (!d) return { dt:null, dateLabel:'', timeLabel:'', iso:'' };
  const t = tRaw && /\d{1,2}:\d{2}/.test(tRaw)
    ? (tRaw.length === 5 ? tRaw + ':00' : tRaw)
    : '23:59:00';
  try{
    const dt = DateTime.fromISO(`${d}T${t}`, { zone: TZ });
    if (!dt.isValid) return { dt:null, dateLabel:'', timeLabel:'', iso:'' };
    const dateLabel = dt.toFormat('MM/dd/yyyy');
    const timeLabel = tRaw ? (dt.toFormat('h:mm a') + ' CT') : '';
    return { dt, dateLabel, timeLabel, iso: dt.toISO() };
  }catch{
    return { dt:null, dateLabel:'', timeLabel:'', iso:'' };
  }
}
function compareByStartTime(a, b){
  const pa = parseDueDateTimeCT(a);
  const pb = parseDueDateTimeCT(b);
  if (!pa.dt && !pb.dt) return 0;
  if (!pa.dt) return 1;
  if (!pb.dt) return -1;
  return pa.dt.toMillis() - pb.dt.toMillis();
}

/* ========= Dictionaries ========= */
const SERVICE_MAP = {
  27: "Water Mitigation",
  28: "Contents",
  29: "Reconstruction",
  37: "Contents",
  38: "Reconstruction",
  39: "Fire Cleanup",
  40: "Biohazard",
  41: "General Cleaning",
  42: "Duct Cleaning"
};
const PRODUCTION_TEAM_MAP = {
  47: "Kings",
  48: "Johnathan",
  49: "Pena",
  50: "Hector",
  51: "Sebastian",
  52: "Anastacio",
  53: "Mike",
  54: "Gary",
  55: "Greg",
  56: "Amber",
  57: "Anna Marie",
  58: "Rosemary",
  59: "Slot 4",
  60: "Slot 5"
};
const PRODUCTION_TEAM_FIELD_KEY = '8bbab3c120ade3217b8738f001033064e803cdef';
const DEAL_ADDRESS_KEY = 'd204334da759b00ceeb544837f8f0f016c9f3e5f';
const PRODUCTION_TEAM_TO_CHANNEL = {
  47: 'C09BXCCD95W', 48: 'C09ASB1N32B', 49: 'C09ASBE36Q7', 50: 'C09B6P5LVPY', 51: 'C09AZ6VT459',
  52: 'C09BA0XUAV7', 53: 'C098H8GU355', 54: 'C09AZ63JEJF', 55: 'C09BFFGBYTB', 56: 'C09B49MJHEE', 57: 'C09B85LE544',
  58: 'C09EQNJN960', 59: null, 60: null
};

/* ========= Slack App (minimal) ========= */
const receiver = new ExpressReceiver({
  signingSecret: SLACK_SIGNING_SECRET,
  endpoints: { events:'/slack/events', commands:'/slack/commands', actions:'/slack/interact' },
  processBeforeResponse: true,
});
const app = new App({ token: SLACK_BOT_TOKEN, signingSecret: SLACK_SIGNING_SECRET, receiver });

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

function _fmt(parts) { return parts.map(s => String(s||'').trim()).filter(Boolean).join(', '); }
function _pickOrgAddress(org = {}) {
  if (org.address && String(org.address).trim()) return String(org.address).trim();
  const parts = [
    org.address_street_number,
    org.address_route,
    org.address_sublocality,
    org.address_locality,
    org.address_admin_area_level_1,
    org.address_postal_code,
    org.address_country
  ];
  const s = _fmt(parts);
  return s || null;
}
async function fetchOrganization(orgRef) {
  const orgId = (orgRef && typeof orgRef === 'object')
    ? (orgRef.value ?? orgRef.id ?? orgRef)
    : orgRef;
  if (!orgId) return null;
  try{
    const r = await fetch(`https://api.pipedrive.com/v1/organizations/${encodeURIComponent(orgId)}?api_token=${PIPEDRIVE_API_TOKEN}`);
    const j = await r.json();
    return (j?.success && j.data) ? j.data : null;
  }catch{ return null; }
}
function _readDealCustomAddress(deal) {
  if (!deal) return null;
  const v = deal[DEAL_ADDRESS_KEY];
  if (!v) return null;
  if (typeof v === 'string') return v.trim() || null;
  if (typeof v === 'object') {
    const formatted = v.formatted_address || v.value || null;
    if (formatted) return String(formatted).trim() || null;
    const parts = [
      v.subpremise, v.street_number, v.route, v.sublocality, v.locality,
      v.admin_area_level_1, v.postal_code, v.country
    ];
    const s = _fmt(parts);
    return s || null;
  }
  return null;
}
async function getBestLocation(deal){
  if (!deal) return 'N/A';
  const org = await fetchOrganization(deal.org_id || deal.organization || null);
  const orgAddr = _pickOrgAddress(org || {});
  if (orgAddr) return orgAddr;
  const dealAddr = _readDealCustomAddress(deal);
  return dealAddr || 'N/A';
}

/* ========= Invoice detection ========= */
function normalizeForInvoice(s=''){
  return String(s).toLowerCase().replace(/\s*\/\s*/g, '/').replace(/\s+/g, ' ').trim();
}
function isInvoiceLike(activity){
  if (!SKIP_INVOICE_TASKS) return false;
  const subjectNorm = normalizeForInvoice(activity?.subject || '');
  const noteNorm    = normalizeForInvoice(htmlToPlainText(activity?.note || ''));
  const EXACTS = (process.env.INVOICE_EXACT_SUBJECTS || 'billed/invoice,invoice,invoice task,collect payment,final invoice,bill in 5 days')
    .split(',').map(s => normalizeForInvoice(s)).filter(Boolean);
  if (EXACTS.includes(subjectNorm)) return true;
  if (subjectNorm.startsWith('invoice:') || subjectNorm.startsWith('invoice -') || subjectNorm.startsWith('billed/')) return true;
  const escaped = INVOICE_KEYWORDS.map(k => k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  const re = new RegExp(`(?:^|\\b)(${escaped.join('|')})(?:\\b|$)`, 'i');
  return re.test(subjectNorm) || re.test(noteNorm);
}

/* ========= Subject renaming in Pipedrive ========= */
function normalizeCrewTag(name){
  if (!name) return null;
  const clean = String(name).trim().replace(/\s+/g,' ');
  return `Crew: ${clean}`;
}
function extractCrewName(subject){
  const m = String(subject || '').match(/Crew:\s*([A-Za-z][A-Za-z ]*)/i);
  return m ? m[1].trim().toLowerCase() : null;
}
function normalizeName(s){ return String(s || '').trim().toLowerCase(); }
function buildRenamedSubject(original, assigneeName){
  const crewTag = normalizeCrewTag(assigneeName);
  if (!crewTag) return original || '';
  const subj = String(original || '').trim();
  const existing = subj.match(/Crew:\s*[A-Za-z][A-Za-z ]*/i)?.[0];
  if (existing) return subj.replace(/Crew:\s*[A-Za-z][A-Za-z ]*/i, crewTag);
  return subj ? `${subj} — ${crewTag}` : crewTag;
}
async function ensureCrewTagMatches(activityId, currentSubject, assigneeName) {
  const want = (assigneeName || '').trim();
  if (!activityId || !want) return { did:false, reason:'no-assignee' };
  const haveCrew = extractCrewName(currentSubject);
  const wantCrew = normalizeName(want);
  if (haveCrew === wantCrew) return { did:false, reason:'already-correct' };
  const newSubject = buildRenamedSubject(currentSubject || '', want);
  if (!newSubject || newSubject === currentSubject) return { did:false, reason:'no-op' };
  console.log(`[RENAME] PUT id=${activityId}: "${currentSubject}" -> "${newSubject}"`);
  const resp = await fetch(
    `https://api.pipedrive.com/v1/activities/${encodeURIComponent(activityId)}?api_token=${PIPEDRIVE_API_TOKEN}`,
    { method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ subject:newSubject }) }
  );
  const j = await resp.json();
  console.log(`[RENAME] PD response id=${activityId} success=${!!j?.success} subject=${JSON.stringify(j?.data?.subject || newSubject)}`);
  return { did: !!j?.success };
}

/* ========= Assignee detection (enum-only, activity first) ========= */
function readEnumId(v){ return (v && typeof v === 'object' && v.value != null) ? v.value : v; }
function detectAssignee({ deal, activity }) {
  const aTid = activity ? readEnumId(activity[PRODUCTION_TEAM_FIELD_KEY]) : null;
  const dTid = (!aTid && ALLOW_DEAL_FALLBACK && deal) ? readEnumId(deal[PRODUCTION_TEAM_FIELD_KEY]) : null;
  const tid  = aTid || dTid || null;
  if (tid && PRODUCTION_TEAM_TO_CHANNEL[tid]) {
    return {
      teamId: String(tid),
      teamName: PRODUCTION_TEAM_MAP[tid] || `Team ${tid}`,
      channelId: PRODUCTION_TEAM_TO_CHANNEL[tid]
    };
  }
  return { teamId: null, teamName: null, channelId: null };
}

/* ========= Reassignment & completion tracking ========= */
const ASSIGNEE_POSTS = new Map(); // activityId -> { assigneeChannelId, messageTs, fileIds: string[] }
const AID_TAG = (id)=>`[AID:${id}]`;

/* ========= Slack helpers (minimal) ========= */
async function ensureBotInChannel(channelId){
  if (!channelId) return;
  try{ await app.client.conversations.join({ channel: channelId }); }
  catch(e){ /* ignore not_in_channel for private we can't join */ }
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
async function deleteAssigneePdfByMarker(activityId, channelId, lookback=400){
  try{
    const h = await app.client.conversations.history({ channel: channelId, limit: lookback, inclusive: true });
    for (const m of (h?.messages||[])){
      const text = (m.text||'') + ' ' + (m.previous_message?.text||'');
      if (text.includes(AID_TAG(activityId))){
        const files = m.files || [];
        for (const f of files){
          try{ await app.client.files.delete({ file: f.id }); } catch(e){}
        }
        try{ await app.client.chat.delete({ channel: channelId, ts: m.ts }); } catch(e){}
      }
    }
  }catch(e){}
}
async function deleteAssigneePost(activityId){
  const key = String(activityId);
  const rec = ASSIGNEE_POSTS.get(key);
  if (rec){
    const { assigneeChannelId, messageTs, fileIds } = rec;
    if (assigneeChannelId && messageTs){
      try { await app.client.chat.delete({ channel: assigneeChannelId, ts: messageTs }); } catch (e){}
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

/* ========= Slack message (simple) ========= */
function stripCrewSuffix(s=''){
  return String(s).replace(/\s*[—-]\s*Crew:\s*[A-Za-z][A-Za-z ]*$/i, '').trim();
}
function buildSummary({ activity, deal, assigneeName, noteText }){
  const dealId = activity.deal_id || (deal?.id ?? 'N/A');
  const dealTitle = deal?.title || 'N/A';
  const serviceId = readEnumId(deal?.['5b436b45b63857305f9691910b6567351b5517bc']);
  const typeOfService = SERVICE_MAP[serviceId] || 'N/A';
  const subjNoCrew = stripCrewSuffix(activity.subject || '');
  return [
    `📌 Work Order • [JOB ${deal?.id || 'N/A'}] ${dealTitle} — ${subjNoCrew} — Crew: ${assigneeName || 'Unassigned'}`,
    `🏷️ Deal: ${dealId} — ${dealTitle}`,
    `📦 ${typeOfService}`,
    assigneeName ? `👷 ${assigneeName}` : null,
    noteText || activity.note ? `\n📜 Notes:\n${htmlToPlainText(noteText || activity.note)}` : null,
    `\nScan the QR in the PDF to complete. ${AID_TAG(activity.id)}`
  ].filter(Boolean).join('\n');
}

/* ========= PDF builders ========= */
async function buildWorkOrderPdfBuffer({ activity, dealTitle, typeOfService, location, channelForQR, assigneeName, customerName, jobNumber }) {
  const completeUrl = makeSignedCompleteUrl({ aid: String(activity.id), did: activity.deal_id ? String(activity.deal_id) : '', cid: channelForQR || DEFAULT_CHANNEL });
  const qrDataUrl = await QRCode.toDataURL(completeUrl);
  const qrBuffer = Buffer.from(qrDataUrl.replace(/^data:image\/png;base64,/, ''), 'base64');
  const doc = new PDFDocument({ margin: 36 });
  const chunks = [];
  doc.on('data', c => chunks.push(c));
  const done = new Promise(r => doc.on('end', () => r(Buffer.concat(chunks))));

  doc.fontSize(20).text('Work Order', { align: 'center' });
  doc.moveDown(0.25);
  doc.fontSize(10).fillColor('#666')
     .text(`Generated: ${DateTime.now().setZone(TZ).toFormat('MM/dd/yyyy, h:mm:ss a')} CT`, { align: 'center' });
  doc.moveDown(1);

  const { dateLabel: dueDateLbl, timeLabel: dueTimeLbl } = parseDueDateTimeCT(activity);

  doc.fillColor('#000').fontSize(12);
  doc.text(`Task: ${activity.subject || '-'}`);
  doc.text(`Due Date:  ${dueDateLbl || '-'}`);
  doc.text(`Start Time: ${dueTimeLbl || '-'}`);
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
  }

  doc.moveDown(0.5);
  doc.font('Helvetica-Bold').text('Scan to Complete');
  doc.font('Helvetica').fontSize(10).fillColor('#555').text('Scanning marks this task complete in Pipedrive.');
  doc.moveDown(0.5);
  doc.image(qrBuffer, { fit: [120, 120] });

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

/* ========= PD file upload ========= */
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

/* ========= Core post (assignee-channel ONLY) ========= */
async function postWorkOrderToAssignee({ activity, deal, assigneeChannelId, assigneeName, noteText }){
  if (!assigneeChannelId || !ENABLE_SLACK_PDF_UPLOAD) return;

  const dealId = activity.deal_id || (deal?.id ?? 'N/A');
  const dealTitle = deal?.title || 'N/A';
  const serviceId = readEnumId(deal?.['5b436b45b63857305f9691910b6567351b5517bc']);
  const typeOfService = SERVICE_MAP[serviceId] || 'N/A';
  const location = await getBestLocation(deal);

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
    activity, dealTitle, typeOfService, location,
    channelForQR: assigneeChannelId || DEFAULT_CHANNEL, // embed assignee channel in QR
    assigneeName, customerName, jobNumber: deal?.id
  });

  const safe = (s) => (s || '').toString().replace(/[^\w\-]+/g,'_').slice(0,60);
  const filename = `WO_${safe(dealTitle)}_${safe(activity.subject)}.pdf`;
  const summary = buildSummary({ activity, deal, assigneeName, noteText });

  await ensureBotInChannel(assigneeChannelId);

  try {
    const up = await uploadPdfToSlack({
      channel: assigneeChannelId,
      filename,
      pdfBuffer,
      title:`Work Order — ${activity.subject || ''}`,
      initialComment: summary
    });
    const fids = [];
    if (up?.files && Array.isArray(up.files)) { for (const f of up.files) if (f?.id) fids.push(f.id); }
    else if (up?.file?.id) { fids.push(up.file.id); }
    const aMsgTs = up?.file?.shares?.public?.[assigneeChannelId]?.[0]?.ts || up?.file?.shares?.private?.[assigneeChannelId]?.[0]?.ts || null;
    ASSIGNEE_POSTS.set(String(activity.id), { assigneeChannelId, messageTs: aMsgTs, fileIds: fids });
  } catch(e){ console.warn('[WO] files.uploadV2 (assignee) failed:', e?.data || e?.message || e); }

  if (ENABLE_PD_FILE_UPLOAD){ try{ await uploadPdfToPipedrive({ dealId, pdfBuffer, filename }); } catch(e){ console.error('[WO] PD upload failed:', e); } }
}

/* ========= Signed QR link ========= */
function makeSignedCompleteUrl({ aid, did='', cid='', ttlSeconds=7*24*60*60 }){
  const exp = Math.floor(Date.now()/1000)+ttlSeconds;
  const raw = `${aid}.${did}.${cid}.${exp}`;
  const params = new URLSearchParams({ aid:String(aid), exp:String(exp), sig:sign(raw) });
  if (did) params.set('did', String(did));
  if (cid) params.set('cid', String(cid));
  return `${cleanBase()}/wo/complete?${params.toString()}`;
}

/* ========= Webhook ========= */
expressApp.post('/pipedrive-task', async (req, res) => {
  try {
    if (!PD_WEBHOOK_KEY || req.query.key !== PD_WEBHOOK_KEY) {
      return res.status(403).send('Forbidden');
    }

    const meta = req.body?.meta || {};
    const rawEntity = (meta.entity || '').toString().toLowerCase();
    const rawAction = (meta.action || meta.event || meta.change || '').toString().toLowerCase();

    const action = /^(create|add|added)$/.test(rawAction) ? 'create'
      : /^(update|updated|change|changed)$/.test(rawAction) ? 'update'
      : /^(delete|deleted)$/.test(rawAction) ? 'delete'
      : rawAction || 'unknown';

    const entity = rawEntity || 'unknown';
    const data = req.body?.data || req.body?.current || null;

    console.log('[PD Hook] entity=%s action=%s id=%s', entity, action, data?.id || meta?.entity_id || 'n/a');

    // ===== Activity create/update =====
    if (entity === 'activity' && (action === 'create' || action === 'update')) {
      if (!data?.id) return res.status(200).send('No activity');
      if (data.done === true || data.done === 1) return res.status(200).send('OK');
      if (!isActivityFresh(data)) return res.status(200).send('OK');
      if (alreadyHandledEvent(meta, data)) return res.status(200).send('OK');

      // fetch the newest activity
      const aRes = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(data.id)}?api_token=${PIPEDRIVE_API_TOKEN}`);
      const aJson = await aRes.json();
      if (!aJson?.success || !aJson.data) return res.status(200).send('Activity fetch fail');
      const activity = aJson.data;

      if (activity.done === true || activity.done === 1) { return res.status(200).send('OK'); }

      // fetch its deal (optional, for PDF content & address)
      let deal = null;
      if (activity.deal_id) {
        const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(activity.deal_id)}?api_token=${PIPEDRIVE_API_TOKEN}`);
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) deal = dJson.data;
      }
      if (deal && !isDealActive(deal)) { return res.status(200).send('OK'); }

      // resolve assignee (activity-only unless ALLOW_DEAL_FALLBACK=true)
      const assignee = detectAssignee({ deal, activity });
      console.log(`[ASSIGNEE/ACT] id=${activity.id} -> ${JSON.stringify(assignee)}`);

      // Pipedrive subject rename (only if not invoice-like)
      if (!isInvoiceLike(activity) && assignee.teamName) {
        await ensureCrewTagMatches(activity.id, activity.subject || '', assignee.teamName);
        // refresh subject for nicer Slack text
        try {
          const a2 = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(activity.id)}?api_token=${PIPEDRIVE_API_TOKEN}`);
          const a2j = await a2.json();
          if (a2j?.success && a2j.data) activity.subject = a2j.data.subject || activity.subject;
        } catch {}
      }

      // Only deliver WO to assignee channel if due today (unless POST_FUTURE_WOS)
      if (!(POST_FUTURE_WOS || isDueTodayCT(activity))) {
        try {
          await deleteAssigneePost(activity.id);
        } catch {}
        return res.status(200).send('OK');
      }

      if (!assignee.channelId) return res.status(200).send('OK'); // nowhere to post

      if (!shouldPostNowStrong(activity, assignee.teamName, deal)) {
        return res.status(200).send('OK');
      }

      await postWorkOrderToAssignee({
        activity, deal,
        assigneeChannelId: assignee.channelId,
        assigneeName: assignee.teamName,
        noteText: activity.note
      });

      return res.status(200).send('OK');
    }

    // ===== Deal update: rename all open activities (no Slack posts here) =====
    if (entity === 'deal' && action === 'update') {
      const dealId = data?.id;
      if (!dealId) return res.status(200).send('No deal');

      // fetch full, current deal
      let deal = null;
      try {
        const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(dealId)}?api_token=${PIPEDRIVE_API_TOKEN}`);
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) deal = dJson.data;
      } catch (e) { console.warn('[PD Hook] DEAL fetch failed; falling back to webhook data', e?.message || e); }
      deal = deal || data;

      // list all OPEN activities on this deal
      const listRes = await fetch(
        `https://api.pipedrive.com/v1/activities?deal_id=${encodeURIComponent(dealId)}&done=0&start=0&limit=50&api_token=${PIPEDRIVE_API_TOKEN}`
      );
      const listJson = await listRes.json();
      const items = (listJson?.data || []).filter(a => a && (a.done === false || a.done === 0) && isActivityFresh(a));

      for (const activity of items) {
        const ass = detectAssignee({ deal, activity }); // still activity-first
        try {
          if (!isInvoiceLike(activity) && ass.teamName) {
            await ensureCrewTagMatches(activity.id, activity.subject || '', ass.teamName);
          }
        } catch (e) {
          console.warn('[WO] rename on DEAL update failed', activity.id, e?.message || e);
        }

        // If a WO was previously posted and date changed out of today, clean it up.
        if (!(POST_FUTURE_WOS || isDueTodayCT(activity))) {
          try { await deleteAssigneePost(activity.id); } catch (e) {}
        }
      }
      return res.status(200).send('OK');
    }

    // ===== Activity delete =====
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

/* ========= 7:00 AM CT daily runner (assignee-channel ONLY) ========= */
expressApp.get('/dispatch/run-7am', async (_req, res) => {
  try {
    const today = DateTime.now().setZone(TZ).toISODate();
    const listRes = await fetch(`https://api.pipedrive.com/v1/activities?done=0&limit=500&api_token=${PIPEDRIVE_API_TOKEN}`);
    const listJson = await listRes.json();
    const all = Array.isArray(listJson?.data) ? listJson.data : [];
    const dueToday = all.filter(a => (a?.due_date||'').trim() === today);
    dueToday.sort(compareByStartTime);

    let posted = 0;
    for (const activity of dueToday) {
      // fresh deal (address, service, etc.)
      let deal = null;
      if (activity.deal_id){
        const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(activity.deal_id)}?api_token=${PIPEDRIVE_API_TOKEN}`);
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) deal = dJson.data;
      }
      if (deal && !isDealActive(deal)) continue;

      const assignee = detectAssignee({ deal, activity });
      if (!assignee.channelId) continue;

      if (!shouldPostNowStrong(activity, assignee.teamName, deal)) continue;

      await postWorkOrderToAssignee({
        activity, deal,
        assigneeChannelId: assignee.channelId,
        assigneeName: assignee.teamName,
        noteText: activity.note
      });
      posted++;
    }
    res.status(200).send(`OK – posted ${posted} due-today WOs`);
  } catch (e) {
    console.error('[7AM] runner error', e?.message||e);
    res.status(500).send('error');
  }
});

/* ========= QR Complete ========= */
expressApp.get('/wo/complete', async (req, res) => {
  try {
    const { aid, did, cid, exp, sig } = req.query || {};
    if (!aid || !exp || !sig) return res.status(400).send('Missing params.');
    const now = Math.floor(Date.now()/1000);
    if (Number(exp) < now) return res.status(410).send('Link expired.');
    const raw = `${aid}.${did || ''}.${cid || ''}.${exp}`;
    if (!verify(raw, sig)) return res.status(403).send('Bad signature.');

    const markedAtIso = new Date().toISOString();
    const pdResp = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(aid)}?api_token=${PIPEDRIVE_API_TOKEN}`, {
      method:'PUT', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ done:true, marked_as_done_time: markedAtIso })
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
            const serviceId = readEnumId(deal?.['5b436b45b63857305f9691910b6567351b5517bc']);
            typeOfService = SERVICE_MAP[serviceId] || 'N/A';
            location = await getBestLocation(deal);
          }
        }
      }
    } catch (e) { console.warn('[WO] fetch for completion PDF failed', e?.message||e); }

    const dealIdForUpload = did || activity?.deal_id;

    try {
      const completedPdf = await buildCompletionPdfBuffer({ activity, dealTitle, typeOfService, location, completedAt: Date.now() });
      if (ENABLE_PD_FILE_UPLOAD) {
        await uploadPdfToPipedrive({ dealId: dealIdForUpload, pdfBuffer: completedPdf, filename: `WO_${aid}_Completed.pdf` });
      }
      // Post completion to the same assignee channel embedded in the QR (cid), else DEFAULT
      const channelForCompletion = cid || DEFAULT_CHANNEL;
      if (ENABLE_SLACK_PDF_UPLOAD && channelForCompletion) {
        await ensureBotInChannel(channelForCompletion);
        await uploadPdfToSlack({
          channel: channelForCompletion,
          filename: `WO_${aid}_Completed.pdf`,
          pdfBuffer: completedPdf,
          title: 'Work Order Completed',
          initialComment: `✅ Completed Work Order for activity ${aid}. ${AID_TAG(aid)}`
        });
      }
    } catch (e) {
      console.error('[WO] completion PDF upload failed', e?.message||e);
    }

    try {
      const when = new Date().toLocaleString();
      const subject = activity?.subject ? `“${activity.subject}”` : '';
      await fetch(`https://api.pipedrive.com/v1/notes?api_token=${PIPEDRIVE_API_TOKEN}` ,{
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({
          deal_id: dealIdForUpload,
          content: `✅ Work Order Completed via QR Scan.\nActivity ${aid} ${subject} marked done at ${when}.\nA completion PDF has been attached to the deal.`
        })
      });
    } catch(e) { console.error('[WO] PD completion note failed', e?.message||e); }

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

    let dealTitle='N/A', typeOfService='N/A', location='N/A', assigneeName=null, deal=null, customerName=null;
    const dealId = data.deal_id;
    if (dealId){
      const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(dealId)}?api_token=${PIPEDRIVE_API_TOKEN}`);
      const dj = await dRes.json();
      if (dj?.success && dj.data){
        deal = dj.data;
        dealTitle = deal.title || 'N/A';
        const serviceId = readEnumId(deal?.['5b436b45b63857305f9691910b6567351b5517bc']);
        typeOfService = SERVICE_MAP[serviceId] || 'N/A';
        location = await getBestLocation(deal);
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

    const pdfBuffer = await buildWorkOrderPdfBuffer({
      activity: data, dealTitle, typeOfService, location,
      channelForQR: DEFAULT_CHANNEL, assigneeName, customerName, jobNumber: deal?.id
    });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="WO_${aid}.pdf"`);
    return res.send(pdfBuffer);
  }catch(e){
    console.error('/wo/pdf error', e);
    res.status(500).send('error');
  }
});

/* ========= Start ========= */
(async () => {
  await app.start(PORT);
  console.log(`✅ Dispatcher running on port ${PORT}`);
})();
