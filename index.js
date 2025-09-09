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
const ALLOW_DEFAULT_FALLBACK = process.env.ALLOW_DEFAULT_FALLBACK !== 'false';
const FORCE_CHANNEL_ID     = process.env.FORCE_CHANNEL_ID || null;
const TZ = 'America/Chicago';

// Feature toggles
const ENABLE_PD_FILE_UPLOAD     = process.env.ENABLE_PD_FILE_UPLOAD     !== 'false';
const ENABLE_PD_NOTE            = false; // only attach the WO PDF, no initial PD note
const ENABLE_SLACK_PDF_UPLOAD   = process.env.ENABLE_SLACK_PDF_UPLOAD   !== 'false';
const ENABLE_DELETE_ON_REASSIGN = process.env.ENABLE_DELETE_ON_REASSIGN !== 'false';
const POST_FUTURE_WOS           = process.env.POST_FUTURE_WOS === 'true';

// Job-channel posting behavior: 'immediate' | 'on_assigned' | 'on_complete'
const JOB_CHANNEL_MODE  = (process.env.JOB_CHANNEL_MODE || 'on_assigned').toLowerCase();
const JOB_CHANNEL_MODES = new Set(['immediate', 'on_assigned', 'on_complete']);
function shouldPostToJobChannel({ assigneeChannelId }){
  const mode = JOB_CHANNEL_MODES.has(JOB_CHANNEL_MODE) ? JOB_CHANNEL_MODE : 'on_assigned';
  if (mode === 'immediate') return true;
  if (mode === 'on_assigned') return !!assigneeChannelId;
  if (mode === 'on_complete') return false;
  return true;
}

// Job-channel content style: 'summary' | 'pdf'
const JOB_CHANNEL_STYLE = (process.env.JOB_CHANNEL_STYLE || 'summary').toLowerCase();

// Skip invoice-like tasks entirely
const SKIP_INVOICE_TASKS = process.env.SKIP_INVOICE_TASKS !== 'false';
const INVOICE_KEYWORDS = (process.env.INVOICE_KEYWORDS || 'invoice,billing,billed,bill,payment request,collect payment,final invoice,send invoice,ar follow up,accounts receivable')
  .split(',').map(s => s.trim().toLowerCase()).filter(Boolean);

/* ===== Subject lists ===== */
function toSubjectListEnv(name){
  return (process.env[name] || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
}
const NEVER_RENAME_SUBJECTS  = toSubjectListEnv('NEVER_RENAME_SUBJECTS');
const NEVER_PROCESS_SUBJECTS = toSubjectListEnv('NEVER_PROCESS_SUBJECTS');

function subjectMatchesList(subject, list){
  const n = normalizeForInvoice(subject || '');
  return list.some(s => normalizeForInvoice(s) === n);
}

// ===== Subject renaming controls & debug =====
const RENAME_ON_ASSIGN = (process.env.RENAME_ON_ASSIGN || 'always').toLowerCase(); // 'never' | 'when_missing' | 'always'
const RENAME_FORMAT    = process.env.RENAME_FORMAT || 'append';
const DEBUG_RENAME     = process.env.DEBUG_RENAME === 'true';
const DISABLE_EVENT_DEDUP = process.env.DISABLE_EVENT_DEDUP === 'true';

// ===== Type-based gating =====
const RENAME_TYPES_ALLOW = (process.env.RENAME_TYPES_ALLOW || '')
  .split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
const RENAME_TYPES_BLOCK = (!process.env.RENAME_TYPES_ALLOW && process.env.RENAME_TYPES_BLOCK)
  ? process.env.RENAME_TYPES_BLOCK.split(',').map(s => s.trim().toLowerCase()).filter(Boolean)
  : [];

// ===== Crew chief auto-invite =====
const INVITE_CREW_CHIEF = process.env.INVITE_CREW_CHIEF === 'true';
const CREW_CHIEF_EMAIL_FIELD_KEY = process.env.CREW_CHIEF_EMAIL_FIELD_KEY || null; // optional PD field on Deal with email
function parseChiefMap(raw=''){
  const map = {};
  for (const pair of raw.split(',').map(s=>s.trim()).filter(Boolean)){
    const [name, idsRaw] = pair.split(':');
    if (!name || !idsRaw) continue;
    map[name.trim().toLowerCase()] = idsRaw.split('|').map(s=>s.trim()).filter(Boolean);
  }
  return map;
}
const TEAM_TO_CHIEF = parseChiefMap(process.env.CREW_CHIEF_MAP || '');

// 🔁 NEW: person-name based fallbacks (e.g., Kim Kay)
function parseNameToChannel(raw=''){
  const out = {};
  for (const pair of raw.split(',').map(s=>s.trim()).filter(Boolean)){
    const [name, chan] = pair.split(':');
    if (!name || !chan) continue;
    out[name.trim().toLowerCase()] = chan.trim();
  }
  return out;
}
function parseNameToSlackMap(raw=''){
  const out = {};
  for (const pair of raw.split(',').map(s=>s.trim()).filter(Boolean)){
    const [name, idsRaw] = pair.split(':');
    if (!name || !idsRaw) continue;
    out[name.trim().toLowerCase()] = idsRaw.split('|').map(s=>s.trim()).filter(Boolean);
  }
  return out;
}
const CHIEF_NAME_TO_CHANNEL      = parseNameToChannel(process.env.CHIEF_NAME_TO_CHANNEL || '');
const CREW_CHIEF_NAME_TO_SLACK   = parseNameToSlackMap(process.env.CREW_CHIEF_NAME_TO_SLACK || '');

/* ===== Required envs ===== */
if (!SLACK_SIGNING_SECRET) throw new Error('Missing SLACK_SIGNING_SECRET');
if (!SLACK_BOT_TOKEN)      throw new Error('Missing SLACK_BOT_TOKEN');
if (!PIPEDRIVE_API_TOKEN)  throw new Error('Missing PIPEDRIVE_API_TOKEN');
if (!SIGNING_SECRET)       throw new Error('Missing WO_QR_SECRET');
if (!BASE_URL)             throw new Error('Missing BASE_URL');
if (!PD_WEBHOOK_KEY) console.warn('⚠️ PD_WEBHOOK_KEY not set; /pipedrive-task will 403.');

// Crash handlers
process.on('unhandledRejection', (r)=>console.error('[FATAL] Unhandled Rejection:', r?.stack||r));
process.on('uncaughtException', (e)=>console.error('[FATAL] Uncaught Exception:', e?.stack||e));

/* ========= Event dedup (webhook loops) ========= */
const MAX_ACTIVITY_AGE_DAYS = Number(process.env.MAX_ACTIVITY_AGE_DAYS || 14);

const EVENT_CACHE = new Map();
const EVENT_CACHE_TTL_MS = 60 * 1000; // 60s

function alreadyHandledEvent(meta, activity){
  const id  = activity?.id || meta?.id || 'n/a';
  const ver = meta?.timestamp || meta?.time || activity?.update_time || activity?.add_time || Date.now();
  const key = `aid:${id}|v:${ver}`;
  const now = Date.now();
  const exp = EVENT_CACHE.get(key);
  if (exp && exp > now) return true;
  if (EVENT_CACHE.size > 1000){
    for (const [k, t] of EVENT_CACHE){ if (t <= now) EVENT_CACHE.delete(k); }
  }
  EVENT_CACHE.set(key, now + EVENT_CACHE_TTL_MS);
  return false;
}

/* ========= Post dedup (stop double PDFs) ========= */
const LAST_POST = new Map();                                     // aid -> timestamp
const POST_DEDUP_MS = Number(process.env.POST_DEDUP_MS || 8000); // 8s window

function shouldPostNow(aid){
  const now = Date.now();
  const last = LAST_POST.get(String(aid)) || 0;
  if (now - last < POST_DEDUP_MS) return false;
  LAST_POST.set(String(aid), now);
  return true;
}

/* ========= Freshness / deal active ========= */
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
function isDueTodayCT(activity){
  const d = (activity?.due_date||'').trim();
  if (!d) return false;
  const today = DateTime.now().setZone(TZ).toISODate();
  return d === today;
}

// === Start-time parsing (CT) + comparator ===
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
    : '23:59:00'; // missing time sorts last

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
const SERVICE_MAP = { 27:'Water Mitigation',28:'Fire Cleanup',29:'Contents',30:'Biohazard',31:'General Cleaning',32:'Duct Cleaning' };
const PRODUCTION_TEAM_MAP = { 47:'Kings',48:'Johnathan',49:'Penabad',50:'Hector',51:'Sebastian',52:'Anastacio',53:'Mike',54:'Gary',55:'Greg',56:'Amber',57:'Anna Marie',58:'Slot 3',59:'Slot 4',60:'Slot 5' };
// PD custom field key (Production Team on DEAL/ACTIVITY)
const PRODUCTION_TEAM_FIELD_KEY = '8bbab3c120ade3217b8738f001033064e803cdef';
// Production Team enum ID → Slack channel
const PRODUCTION_TEAM_TO_CHANNEL = {
  47: 'C09BXCCD95W', 48: 'C09ASB1N32B', 49: 'C09ASBE36Q7', 50: 'C09B6P5LVPY', 51: 'C09AZ6VT459',
  52: 'C09BA0XUAV7', 53: 'C098H8GU355', 54: 'C09AZ63JEJF', 55: 'C09BFFGBYTB', 56: 'C09B49MJHEE', 57: 'C09B85LE544',
  58: null, 59: null, 60: null
};
// Fallback name→channel for parsing "Crew: Name"
const NAME_TO_CHANNEL = {
  anastacio:'C09BA0XUAV7', mike:'C098H8GU355', greg:'C09BFFGBYTB', amber:'C09B49MJHEE', 'anna marie':'C09B85LE544', annamarie:'C09B85LE544',
  kings:'C09BXCCD95W', penabad:'C09ASBE36Q7', johnathan:'C09ASB1N32B', gary:'C09AZ63JEJF', hector:'C09B6P5LVPY', sebastian:'C09AZ6VT459'
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
function isBilledInvoiceSubject(subject){
  const n = normalizeForInvoice(subject || '');
  return n === 'billed/invoice' || n.startsWith('billed/invoice');
}

/* ========= Type gating ========= */
function getActivityTypeKey(a){ return String(a?.type || '').toLowerCase().trim(); }
function isTypeAllowedForRename(activity){
  const t = getActivityTypeKey(activity);
  if (!t) return true;
  if (RENAME_TYPES_ALLOW.length) return RENAME_TYPES_ALLOW.includes(t);
  if (RENAME_TYPES_BLOCK.length) return !RENAME_TYPES_BLOCK.includes(t);
  return true;
}

/* ========= Subject renaming ========= */
function normalizeCrewTag(name){
  if (!name) return null;
  const clean = String(name).trim().replace(/\s+/g,' ');
  return `Crew: ${clean}`;
}
function extractCrewTag(subject){
  if (!subject) return null;
  const m = String(subject).match(/Crew:\s*([A-Za-z][A-Za-z ]*)/i);
  return m?.[0] || null;
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
  const existing = extractCrewTag(subj);
  if (existing) return subj.replace(/Crew:\s*[A-Za-z][A-Za-z ]*/i, crewTag);
  if (RENAME_FORMAT === 'append') return subj ? `${subj} — ${crewTag}` : crewTag;
  return subj ? `${subj} — ${crewTag}` : crewTag;
}

// Only PUT when needed, with loud logs
async function ensureCrewTagMatches(activityId, currentSubject, assigneeName) {
  const want = (assigneeName || '').trim();
  if (!activityId || !want) return { did:false, reason:'no-assignee' };

  const haveCrew = extractCrewName(currentSubject);
  const wantCrew = normalizeName(want);

  if (DEBUG_RENAME) {
    console.log(`[RENAME] check id=${activityId} have=${haveCrew} want=${wantCrew} subj=${JSON.stringify(currentSubject)}`);
  }
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
  catch(e){ if (DEBUG_RENAME) console.warn('[WARN] bolt-app', e?.data?.error || e?.message || e); }
}
async function resolveDealChannelId({ dealId, allowDefault = ALLOW_DEFAULT_FALLBACK }){
  const byDeal = await findDealChannelId(dealId);
  if (byDeal) return byDeal;
  return allowDefault ? DEFAULT_CHANNEL : null;
}

/* ========= Assignee detection (robust to {value}) ========= */
function readEnumId(v){ return (v && typeof v === 'object' && v.value != null) ? v.value : v; }
function detectAssignee({ deal, activity }) {
  // ONLY use the Production Team enum (activity first, then deal)
  const aTid = activity ? readEnumId(activity[PRODUCTION_TEAM_FIELD_KEY]) : null;
  const dTid = !aTid && deal ? readEnumId(deal[PRODUCTION_TEAM_FIELD_KEY]) : null;
  const tid  = aTid || dTid || null;

  if (tid && PRODUCTION_TEAM_TO_CHANNEL[tid]) {
    return {
      teamId: String(tid),
      teamName: PRODUCTION_TEAM_MAP[tid] || `Team ${tid}`,
      channelId: PRODUCTION_TEAM_TO_CHANNEL[tid]
    };
  }

  // No fallbacks
  return { teamId: null, teamName: null, channelId: null };
}

  // 2) Deal field (enum)
  if (deal) {
    const raw = deal[PRODUCTION_TEAM_FIELD_KEY];
    const dTid = readEnumId(raw);
    if (dTid && PRODUCTION_TEAM_TO_CHANNEL[dTid]) {
      return { teamId: String(dTid), teamName: PRODUCTION_TEAM_MAP[dTid] || `Team ${dTid}`, channelId: PRODUCTION_TEAM_TO_CHANNEL[dTid] };
    }
  }
  // 3) Fallback via "Crew: Name" in title/subject
  const crewFrom = (s) => (s ? (String(s).match(/Crew:\s*([A-Za-z][A-Za-z ]*)/i)?.[1] || null) : null);
  const name = crewFrom(deal?.title) || crewFrom(activity?.subject);
  if (name){
    const key = name.toLowerCase();
    const channelId = NAME_TO_CHANNEL[key] || CHIEF_NAME_TO_CHANNEL[key] || null;
    const teamId = NAME_TO_TEAM_ID[key] || null;
    const teamName = PRODUCTION_TEAM_MAP[teamId] || (name.charAt(0).toUpperCase()+name.slice(1));
    return { teamId: teamId ? String(teamId) : null, teamName, channelId };
  }
  // 4) NEW: person-assignee fallback from activity owner/user
  const personName =
    activity?.user_id?.name ||
    activity?.assigned_to_user_id?.name ||
    activity?.owner_name ||
    null;
  if (personName){
    const key = normalizeName(personName);
    const channelId = CHIEF_NAME_TO_CHANNEL[key] || NAME_TO_CHANNEL[key] || null;
    return { teamId: null, teamName: personName, channelId };
  }
  return { teamId:null, teamName:null, channelId:null };
}

/* ========= Reassignment & completion tracking ========= */
const ASSIGNEE_POSTS = new Map(); // activityId -> { assigneeChannelId, messageTs, fileIds: string[] }
const AID_TAG = (id)=>`[AID:${id}]`;

/* ========= PDF builders ========= */
async function buildWorkOrderPdfBuffer({ activity, dealTitle, typeOfService, location, channelForQR, assigneeName, customerName, jobNumber }) {
  const completeUrl = makeSignedCompleteUrl({ aid: String(activity.id), did: activity.deal_id ? String(activity.deal_id) : '', cid: channelForQR || DEFAULT_CHANNEL });
  const qrDataUrl = await QRCode.toDataURL(completeUrl);
  const qrBuffer = Buffer.from(qrDataUrl.replace(/^data:image\/png;base64,/, ''), 'base64');
  const doc = new PDFDocument({ margin: 36 });
  const chunks = [];
  doc.on('data', c => chunks.push(c));
  const done = new Promise(r => doc.on('end', () => r(Buffer.concat(chunks))));

  // Header
  doc.fontSize(20).text('Work Order', { align: 'center' });
  doc.moveDown(0.25);
  // Generated timestamp (CT)
  doc.fontSize(10).fillColor('#666')
     .text(`Generated: ${DateTime.now().setZone(TZ).toFormat('MM/dd/yyyy, h:mm:ss a')} CT`, { align: 'center' });
  doc.moveDown(1);

  // Parse due_date + due_time
  const { dateLabel: dueDateLbl, timeLabel: dueTimeLbl } = parseDueDateTimeCT(activity);

  // Body
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

  // (No raw URL printed)
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

/* ========= Slack message builders ========= */
function stripCrewSuffix(s=''){
  // remove "— Crew: Name" or "- Crew: Name" at the end
  return String(s).replace(/\s*[—-]\s*Crew:\s*[A-Za-z][A-Za-z ]*$/i, '').trim();
}
function buildSummary({ activity, deal, assigneeName, noteText }){
  const dealId = activity.deal_id || (deal?.id ?? 'N/A');
  const dealTitle = deal?.title || 'N/A';
  const serviceId = deal ? deal['5b436b45b63857305f9691910b6567351b5517bc'] : null;
  const typeOfService = SERVICE_MAP[serviceId] || serviceId || 'N/A';
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
function buildJobChannelNotice({ activity, deal, assigneeName }){
  const subj = activity?.subject || 'Task';
  const who  = assigneeName || 'Unassigned';
  return [
    `🧭 *${subj}* has been assigned to *${who}*. ${AID_TAG(activity.id)}`,
    `📨 Work order delivered to ${who === 'Unassigned' ? 'assignee channel' : `*${who}*'s channel`}.`
  ].join('\n');
}

/* ========= Invite helpers ========= */
async function inviteUsersToChannel(channelId, userIds=[]){
  if (!channelId || !userIds.length) return;
  try {
    await app.client.conversations.invite({ channel: channelId, users: userIds.join(',') });
  } catch (e) {
    console.warn('[INVITE] invite failed:', e?.data?.error || e?.message || e);
  }
}

async function resolveChiefSlackIds({ assigneeName, deal }){
  const out = new Set();
  const key = String(assigneeName || '').trim().toLowerCase();
  // team-based mapping
  for (const id of (TEAM_TO_CHIEF[key] || [])) out.add(id);
  // name-based mapping 🔁 NEW
  for (const id of (CREW_CHIEF_NAME_TO_SLACK[key] || [])) out.add(id);

  if (CREW_CHIEF_EMAIL_FIELD_KEY && deal?.[CREW_CHIEF_EMAIL_FIELD_KEY]){
    const email = (typeof deal[CREW_CHIEF_EMAIL_FIELD_KEY] === 'object' && deal[CREW_CHIEF_EMAIL_FIELD_KEY].value)
      ? deal[CREW_CHIEF_EMAIL_FIELD_KEY].value
      : deal[CREW_CHIEF_EMAIL_FIELD_KEY];
    if (email && /@/.test(String(email))){
      try {
        const u = await app.client.users.lookupByEmail({ email: String(email).trim() });
        if (u?.user?.id) out.add(u.user.id);
      } catch (e) {
        console.warn('[INVITE] lookupByEmail failed:', email, e?.data?.error || e?.message || e);
      }
    }
  }
  return [...out];
}

/* ========= Core post ========= */
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
function getTimeField(v){ if(!v) return ''; if(typeof v==='string') return v; if(typeof v==='object' && v.value) return String(v.value); return ''; }

async function postWorkOrderToChannels({ activity, deal, jobChannelId, assigneeChannelId, assigneeName, noteText }){
  const dealId = activity.deal_id || (deal?.id ?? 'N/A');
  const dealTitle = deal?.title || 'N/A';
  const serviceId = deal ? deal['5b436b45b63857305f9691910b6567351b5517bc'] : null;
  const typeOfService = SERVICE_MAP[serviceId] || serviceId || 'N/A';
  const location = deal?.location || 'N/A';

  // Keep a copy for invitations even if we suppress duplicate posting later
  const inviteChannelId = jobChannelId || null;

  // If job and assignee channels are identical, post only once (prefer assignee channel/PDF)
  if (jobChannelId && assigneeChannelId && jobChannelId === assigneeChannelId) {
    jobChannelId = null;
  }

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
    channelForQR: (inviteChannelId || DEFAULT_CHANNEL), assigneeName,
    customerName, jobNumber: deal?.id
  });

  const safe = (s) => (s || '').toString().replace(/[^\w\-]+/g,'_').slice(0,60);
  const filename = `WO_${safe(dealTitle)}_${safe(activity.subject)}.pdf`;
  const summary = buildSummary({ activity, deal, assigneeName, noteText });

  // 🚀 auto-invite crew chief to the job/deal channel
  if (INVITE_CREW_CHIEF && inviteChannelId){
    await ensureBotInChannel(inviteChannelId);
    try {
      const chiefIds = await resolveChiefSlackIds({ assigneeName, deal });
      if (chiefIds.length) await inviteUsersToChannel(inviteChannelId, chiefIds);
    } catch (e) {
      console.warn('[INVITE] chief invite error:', e?.message || e);
    }
  }

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

  // Assignee channel
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

    console.log('[PD Hook] entity=%s action=%s id=%s', entity, action, data?.id || meta?.entity_id || 'n/a');

    // ===== Activity create/update =====
    if (entity === 'activity' && (action === 'create' || action === 'update')) {
      if (!data?.id) return res.status(200).send('No activity');

      if (data.done === true || data.done === 1) { return res.status(200).send('OK'); }
      if (!isActivityFresh(data)) { return res.status(200).send('OK'); }
      if (!DISABLE_EVENT_DEDUP && alreadyHandledEvent(meta, data)) { return res.status(200).send('OK'); }

      // fetch the newest activity
      const aRes = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(data.id)}?api_token=${PIPEDRIVE_API_TOKEN}`);
      const aJson = await aRes.json();
      if (!aJson?.success || !aJson.data) return res.status(200).send('Activity fetch fail');
      const activity = aJson.data;

      if (activity.done === true || activity.done === 1) { return res.status(200).send('OK'); }

      // fetch its deal (optional)
      let deal = null;
      if (activity.deal_id) {
        const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(activity.deal_id)}?api_token=${PIPEDRIVE_API_TOKEN}`);
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) deal = dJson.data;
      }
      if (deal && !isDealActive(deal)) { return res.status(200).send('OK'); }

      // resolve channels/assignee
      let jobChannelId = await resolveDealChannelId({ dealId: activity.deal_id, allowDefault: ALLOW_DEFAULT_FALLBACK });
      if (FORCE_CHANNEL_ID) jobChannelId = FORCE_CHANNEL_ID;
      const assignee = detectAssignee({ deal, activity });
      console.log(`[ASSIGNEE/ACT] id=${activity.id} -> ${JSON.stringify(assignee)}`);

      // rename policy (optional note without subject change for invoice-like)
      if (isBilledInvoiceSubject(activity.subject)) {
        if (assignee.teamName && ENABLE_PD_NOTE) {
          try {
            await fetch(`https://api.pipedrive.com/v1/notes?api_token=${PIPEDRIVE_API_TOKEN}`,{
              method:'POST', headers:{'Content-Type':'application/json'},
              body: JSON.stringify({ content:`🧾 Crew assignment update (no subject change): ${assignee.teamName}`, pinned_to_activity_id: String(activity.id) })
            });
          } catch(e){ console.warn('[WO] logCrewHistoryNote failed', activity.id, e?.message||e); }
        }
      } else if (assignee.teamName && isTypeAllowedForRename(activity) && !isInvoiceLike(activity)) {
        await ensureCrewTagMatches(activity.id, activity.subject || '', assignee.teamName);
        // refresh subject for nicer Slack text
        try {
          const a2 = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(activity.id)}?api_token=${PIPEDRIVE_API_TOKEN}`);
          const a2j = await a2.json();
          if (a2j?.success && a2j.data) activity.subject = a2j.data.subject || activity.subject;
        } catch {}
      }

      // Only post if due today (unless POST_FUTURE_WOS); otherwise clean up any prior post
      if (!(POST_FUTURE_WOS || isDueTodayCT(activity))) {
        console.log('[WO] deferred (not due today): aid=%s due=%s → cleaning up', activity.id, activity.due_date);
        try {
          await deleteAssigneePost(activity.id);
          const jobCh = await resolveDealChannelId({ dealId: activity.deal_id, allowDefault: ALLOW_DEFAULT_FALLBACK });
          if (jobCh) await deleteAssigneePdfByMarker(activity.id, jobCh, 400);
        } catch (e) { console.warn('[WO] cleanup on date-change failed', e?.message || e); }
        return res.status(200).send('OK');
      }

      if (!shouldPostNow(activity.id)) { return res.status(200).send('OK'); }

      await postWorkOrderToChannels({
        activity, deal, jobChannelId,
        assigneeChannelId: assignee.channelId,
        assigneeName: assignee.teamName,
        noteText: activity.note
      });

      return res.status(200).send('OK');
    }

    // ===== Deal update: re-assign/rename all open activities =====
    if (entity === 'deal' && action === 'update') {
      const dealId = data?.id;
      if (!dealId) return res.status(200).send('No deal');

      // fetch full, current deal (webhook diffs often omit custom fields)
      let deal = null;
      try {
        const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(dealId)}?api_token=${PIPEDRIVE_API_TOKEN}`);
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) deal = dJson.data;
      } catch (e) {
        console.warn('[PD Hook] DEAL fetch failed; falling back to webhook data', e?.message || e);
      }
      deal = deal || data;

      const assignee = detectAssignee({ deal, activity: null });
      let jobChannelId = await resolveDealChannelId({ dealId, allowDefault: ALLOW_DEFAULT_FALLBACK });
      if (FORCE_CHANNEL_ID) jobChannelId = FORCE_CHANNEL_ID;

      const listRes = await fetch(
        `https://api.pipedrive.com/v1/activities?deal_id=${encodeURIComponent(dealId)}&done=0&start=0&limit=50&api_token=${PIPEDRIVE_API_TOKEN}`
      );
      const listJson = await listRes.json();
      const items = (listJson?.data || []).filter(a => a && (a.done === false || a.done === 0) && isActivityFresh(a));

      for (const activity of items) {
        // Only post immediately if due today — otherwise clean up any prior post
        if (!(POST_FUTURE_WOS || isDueTodayCT(activity))) {
          console.log('[WO] deferred (deal update; not due today): aid=%s due=%s → cleaning up', activity.id, activity.due_date);
          try {
            await deleteAssigneePost(activity.id);
            const jobCh = await resolveDealChannelId({ dealId: activity.deal_id, allowDefault: ALLOW_DEFAULT_FALLBACK });
            if (jobCh) await deleteAssigneePdfByMarker(activity.id, jobCh, 400);
          } catch (e) { console.warn('[WO] cleanup on deal-update date-change failed', e?.message || e); }
          continue;
        }

        if (!shouldPostNow(activity.id)) { continue; }

        await postWorkOrderToChannels({
          activity, deal, jobChannelId,
          assigneeChannelId: assignee.channelId,
          assigneeName: assignee.teamName,
          noteText: activity.note
        });
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

/* ========= 7:00 AM CT daily runner ========= */
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
      // refresh deal & assignee like webhook path
      let deal = null;
      if (activity.deal_id){
        const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(activity.deal_id)}?api_token=${PIPEDRIVE_API_TOKEN}`);
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) deal = dJson.data;
      }
      if (deal && !isDealActive(deal)) continue;

      let jobChannelId = await resolveDealChannelId({ dealId: activity.deal_id, allowDefault: ALLOW_DEFAULT_FALLBACK });
      if (FORCE_CHANNEL_ID) jobChannelId = FORCE_CHANNEL_ID;
      const assignee = detectAssignee({ deal, activity });

      if (!shouldPostNow(activity.id)) continue;

      await postWorkOrderToChannels({
        activity, deal, jobChannelId,
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
            const serviceId = deal['5b436b45b63857305f9691910b6567351b5517bc'];
            typeOfService = SERVICE_MAP[serviceId] || serviceId || 'N/A';
            location = deal.location || 'N/A';
          }
        }
      }
    } catch (e) { console.warn('[WO] fetch for completion PDF failed', e?.message||e); }

    const dealIdForUpload = did || activity?.deal_id;

    try {
      const completedPdf = await buildCompletionPdfBuffer({ activity, dealTitle, typeOfService, location, completedAt: Date.now() });
      const up = await uploadPdfToPipedrive({ dealId: dealIdForUpload, pdfBuffer: completedPdf, filename: `WO_${aid}_Completed.pdf` });
      console.log('[WO] completion PDF uploaded', up?.success);
      const jobChannel = cid || await resolveDealChannelId({ dealId: dealIdForUpload, allowDefault: ALLOW_DEFAULT_FALLBACK }) || DEFAULT_CHANNEL;
      if (jobChannel) {
        await uploadPdfToSlack({ channel: jobChannel, filename: `WO_${aid}_Completed.pdf`, pdfBuffer: completedPdf, title: 'Work Order Completed', initialComment: `✅ Completed Work Order for activity ${aid}. ${AID_TAG(aid)}` });
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
