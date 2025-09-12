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

/* ===== Subject lists ===== */
function toSubjectListEnv(name){
  return (process.env[name] || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
}
const NEVER_RENAME_SUBJECTS  = toSubjectListEnv('NEVER_RENAME_SUBJECTS');
const NEVER_PROCESS_SUBJECTS = toSubjectListEnv('NEVER_PROCESS_SUBJECTS');

function normalizeForInvoice(s=''){
  return String(s).toLowerCase().replace(/\s*\/\s*/g, '/').replace(/\s+/g, ' ').trim();
}

// Strip trailing "— Crew: Name" so we compare against the base subject
function subjectSansCrew(subject=''){
  return String(subject).replace(/\s*[—-]\s*Crew:\s*[-A-Za-z' ]+$/i, '').trim();
}
function subjectMatchesList(subject, list){
  const base = normalizeForInvoice(subjectSansCrew(subject || ''));
  return list.some(s => normalizeForInvoice(s) === base);
}

// ===== Subject renaming controls & debug =====
const RENAME_ON_ASSIGN = (process.env.RENAME_ON_ASSIGN || 'always').toLowerCase(); // 'never' | 'when_missing' | 'always'
const RENAME_FORMAT    = process.env.RENAME_FORMAT || 'append';
const DEBUG_RENAME     = process.env.DEBUG_RENAME === 'true';
const DISABLE_EVENT_DEDUP = process.env.DISABLE_EVENT_DEDUP === 'true';

// Extra debug verbosity for rename path
const RENAME_DEBUG_LEVEL = Number(process.env.RENAME_DEBUG_LEVEL || (DEBUG_RENAME ? 2 : 0));
function dbgRename(step, payload) {
  if (!RENAME_DEBUG_LEVEL) return;
  try { console.log(`[RENAME][${step}]`, JSON.stringify(payload)); }
  catch { console.log(`[RENAME][${step}]`, payload); }
}

// ===== Type-based gating =====
const RENAME_TYPES_ALLOW = (process.env.RENAME_TYPES_ALLOW || '')
  .split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
const RENAME_TYPES_BLOCK = (!process.env.RENAME_TYPES_ALLOW && process.env.RENAME_TYPES_BLOCK)
  ? process.env.RENAME_TYPES_BLOCK.split(',').map(s => s.trim().toLowerCase()).filter(Boolean)
  : [];

// ===== Crew chief auto-invite (optional) =====
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

function parseNameToSlackMap(raw=''){
  const out = {};
  for (const pair of raw.split(',').map(s=>s.trim()).filter(Boolean)){
    const [name, idsRaw] = pair.split(':');
    if (!name || !idsRaw) continue;
    out[name.trim().toLowerCase()] = idsRaw.split('|').map(s=>s.trim()).filter(Boolean);
  }
  return out;
}
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

const EVENT_CACHE = new Map();           // key -> exp
const EVENT_CACHE_TTL_MS = 60 * 1000;    // 60s

function makeEventKey(meta = {}, activity = {}) {
  const id   = activity?.id || meta?.id || meta?.entity_id || 'n/a';
  const ts   = meta?.timestamp || meta?.time || '';
  const req  = meta?.request_id || meta?.requestId || '';
  const upd  = activity?.update_time || '';
  const done = activity?.done ? '1' : '0';
  const bucket = Math.floor(Date.now() / 10_000); // 10s bucket
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

/* ========= Post dedup (stop double PDFs) ========= */
const POST_FINGERPRINT = new Map(); // aid -> { fp, exp }
const POST_FP_TTL_MS   = Number(process.env.POST_FP_TTL_MS || 10 * 60 * 1000); // default 10m

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
  for (const [k, v] of POST_FINGERPRINT) {
    if (!v || v.exp <= now) POST_FINGERPRINT.delete(k);
  }
  const prev = POST_FINGERPRINT.get(id);
  if (prev && prev.fp === fp) return false;
  POST_FINGERPRINT.set(id, { fp, exp: now + POST_FP_TTL_MS });
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

// PD custom field keys
const PRODUCTION_TEAM_FIELD_KEY = '8bbab3c120ade3217b8738f001033064e803cdef';
const DEAL_ADDRESS_KEY          = 'd204334da759b00ceeb544837f8f0f016c9f3e5f';

// Production Team enum ID → Slack channel
const PRODUCTION_TEAM_TO_CHANNEL = {
  47: 'C09BXCCD95W', 48: 'C09ASB1N32B', 49: 'C09ASBE36Q7', 50: 'C09B6P5LVPY', 51: 'C09AZ6VT459',
  52: 'C09BA0XUAV7', 53: 'C098H8GU355', 54: 'C09AZ63JEJF', 55: 'C09BFFGBYTB', 56: 'C09B49MJHEE', 57: 'C09B85LE544',
  58: 'C09EQNJN960', 59: null, 60: null
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

/* ========= Address helpers ========= */
function _fmt(parts) {
  return parts.map(s => String(s||'').trim()).filter(Boolean).join(', ');
}
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
  const m = String(subject).match(/Crew:\s*([-A-Za-z' ]+)/i);
  return m?.[0] || null;
}
function extractCrewName(subject){
  const m = String(subject || '').match(/Crew:\s*([-A-Za-z' ]+)/i);
  return m ? m[1].trim().toLowerCase().replace(/\s+/g,' ') : null;
}
function normalizeName(s){ return String(s || '').trim().toLowerCase(); }

function buildRenamedSubject(original, assigneeName){
  const crewTag = normalizeCrewTag(assigneeName);
  if (!crewTag) return original || '';
  const subj = String(original || '').trim();
  const existing = extractCrewTag(subj);
  if (existing) return subj.replace(/Crew:\s*([-A-Za-z' ]+)/i, crewTag);
  if (RENAME_FORMAT === 'append') return subj ? `${subj} — ${crewTag}` : crewTag;
  return subj ? `${subj} — ${crewTag}` : crewTag;
}

async function pdPutSubject(activityId, newSubject, attempts=2){
  let last;
  for (let i=0; i<attempts; i++){
    const resp = await fetch(
      `https://api.pipedrive.com/v1/activities/${encodeURIComponent(activityId)}?api_token=${PIPEDRIVE_API_TOKEN}`,
      { method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ subject:newSubject }) }
    );
    const ok = resp.ok;
    const j = await resp.json().catch(()=>null);
    last = { ok, j };
    if (ok && j?.success) return j;
    if (resp.status === 429 || resp.status >= 500) { await new Promise(r=>setTimeout(r, 400)); continue; }
    break;
  }
  return last?.j || { success:false };
}

async function ensureCrewTagMatches(activityId, currentSubject, assigneeName) {
  const want = (assigneeName || '').trim();
  if (!activityId || !want) return { did:false, reason:'no-assignee' };

  const haveCrew = extractCrewName(currentSubject);
  const wantCrew = normalizeName(want);

  if (DEBUG_RENAME) {
    console.log(`[RENAME] check id=${activityId} have=${haveCrew} want=${wantCrew} subj=${JSON.stringify(currentSubject)}`);
  }

  // RENAME_ON_ASSIGN policy
  if (RENAME_ON_ASSIGN === 'never') return { did:false, reason:'policy-never' };
  if (RENAME_ON_ASSIGN === 'when_missing' && haveCrew) return { did:false, reason:'has-crew' };

  if (haveCrew === wantCrew) return { did:false, reason:'already-correct' };

  const newSubject = buildRenamedSubject(currentSubject || '', want);
  if (!newSubject || newSubject === currentSubject) return { did:false, reason:'no-op' };

  console.log(`[RENAME] PUT id=${activityId}: "${currentSubject}" -> "${newSubject}"`);
  const putJ = await pdPutSubject(activityId, newSubject, 3);
  if (!putJ?.success) return { did:false, reason:'pd-put-failed' };

  // Fresh read to collapse races
  try {
    const a2 = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(activityId)}?api_token=${PIPEDRIVE_API_TOKEN}`);
    const a2j = await a2.json();
    const subjNow = a2j?.data?.subject || newSubject;
    return { did:true, subject:subjNow };
  } catch {
    return { did:true, subject:newSubject };
  }
}

/* ========= Assignee detection (enum + fallbacks) ========= */
function readEnumId(v){ return (v && typeof v === 'object' && v.value != null) ? v.value : v; }

// Build a quick lowercase name lookup from your PRODUCTION_TEAM_MAP values
const TEAM_NAME_SET = new Set(
  Object.values(PRODUCTION_TEAM_MAP).map(v => String(v || '').trim().toLowerCase()).filter(Boolean)
);

/**
 * Detects assignee/crew for:
 *   1) Activity-level Production Team enum (preferred)
 *   2) Deal-level Production Team enum (fallback)
 *   3) Activity owner / assigned user name (last resort)
 *
 * Returns { teamId, teamName, channelId }
 */
function detectAssignee({ deal, activity, allowDealFallback = true }) {
  // 1) Activity-level enum
  const aTid = activity ? readEnumId(activity[PRODUCTION_TEAM_FIELD_KEY]) : null;
  // 2) Deal-level enum (if allowed)
  const dTid = (!aTid && allowDealFallback && deal) ? readEnumId(deal[PRODUCTION_TEAM_FIELD_KEY]) : null;
  const tid  = aTid || dTid || null;

  if (tid) {
    const teamName  = PRODUCTION_TEAM_MAP[tid] || `Team ${tid}`;
    const channelId = PRODUCTION_TEAM_TO_CHANNEL[tid] || null;
    return { teamId: String(tid), teamName, channelId };
  }

  // 3) Fallback: infer from activity owner / assigned user
  const userObj =
    (activity && typeof activity.user_id === 'object' ? activity.user_id : null) ||
    (activity && typeof activity.assigned_to_user_id === 'object' ? activity.assigned_to_user_id : null) ||
    null;

  const ownerName =
    (userObj && (userObj.name || userObj.email)) ||
    (activity ? activity.owner_name : null) ||
    null;

  const ownerNameNorm = String(ownerName || '').trim().toLowerCase();

  if (ownerName && TEAM_NAME_SET.has(ownerNameNorm)) {
    return { teamId: null, teamName: ownerName, channelId: null };
  }

  return { teamId: null, teamName: null, channelId: null };
}

/* ========= Minimal channel resolution ========= */
async function resolveDealChannelId({ allowDefault = ALLOW_DEFAULT_FALLBACK }){
  if (FORCE_CHANNEL_ID) return FORCE_CHANNEL_ID;
  return allowDefault ? DEFAULT_CHANNEL : null;
}

async function ensureBotInChannel(channelId){
  if (!channelId) return;
  try { await app.client.conversations.join({ channel: channelId }); }
  catch (e) { /* ignore join errors */ }
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
  doc.fontSize(10).fillColor('#666')
     .text(`Generated: ${DateTime.now().setZone(TZ).toFormat('MM/dd/yyyy, h:mm:ss a')} CT`, { align: 'center' });
  doc.moveDown(1);

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
          try{ await app.client.files.delete({ file: f.id }); } catch(e){ /* ignore */ }
        }
        try{ await app.client.chat.delete({ channel: channelId, ts: m.ts }); } catch(e){ /* ignore */ }
      }
    }
  }catch(e){ /* ignore */ }
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
  return String(s).replace(/\s*[—-]\s*Crew:\s*[A-Za-z][A-Za-z ']*$/i, '').trim();
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
function buildJobChannelNotice({ activity, assigneeName }){
  const subj = activity?.subject || 'Task';
  const who  = assigneeName || 'Unassigned';
  return [
    `🧭 *${subj}* has been assigned to *${who}*. ${AID_TAG(activity.id)}`,
    `📨 Work order delivered to ${who === 'Unassigned' ? 'assignee channel' : `*${who}*'s channel`}.`
  ].join('\n');
}

/* ========= Invite helpers (optional) ========= */
async function inviteUsersToChannel(channelId, userIds=[]){
  if (!channelId || !userIds.length) return;
  try {
    await app.client.conversations.invite({ channel: channelId, users: userIds.join(',') });
  } catch (e) { /* ignore */ }
}

async function resolveChiefSlackIds({ assigneeName, deal }){
  const out = new Set();
  const key = String(assigneeName || '').trim().toLowerCase();
  for (const id of (TEAM_TO_CHIEF[key] || [])) out.add(id);
  for (const id of (CREW_CHIEF_NAME_TO_SLACK[key] || [])) out.add(id);

  if (CREW_CHIEF_EMAIL_FIELD_KEY && deal?.[CREW_CHIEF_EMAIL_FIELD_KEY]){
    const email = (typeof deal[CREW_CHIEF_EMAIL_FIELD_KEY] === 'object' && deal[CREW_CHIEF_EMAIL_FIELD_KEY].value)
      ? deal[CREW_CHIEF_EMAIL_FIELD_KEY].value
      : deal[CREW_CHIEF_EMAIL_FIELD_KEY];
    if (email && /@/.test(String(email))){
      try {
        const u = await app.client.users.lookupByEmail({ email: String(email).trim() });
        if (u?.user?.id) out.add(u.user.id);
      } catch (e) { /* ignore */ }
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

async function postWorkOrderToChannels({ activity, deal, jobChannelId, assigneeChannelId, assigneeName, noteText }){
  const dealId = activity.deal_id || (deal?.id ?? 'N/A');
  const dealTitle = deal?.title || 'N/A';
  const serviceId = readEnumId(deal?.['5b436b45b63857305f9691910b6567351b5517bc']);
  const typeOfService = SERVICE_MAP[serviceId] || 'N/A';
  const location = await getBestLocation(deal);

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
    channelForQR: (jobChannelId || assigneeChannelId || DEFAULT_CHANNEL), assigneeName,
    customerName, jobNumber: deal?.id
  });

  const safe = (s) => (s || '').toString().replace(/[^\w\-]+/g,'_').slice(0,60);
  const filename = `WO_${safe(dealTitle)}_${safe(activity.subject)}.pdf`;
  const summary = buildSummary({ activity, deal, assigneeName, noteText });

  // optional: invite chiefs
  if (INVITE_CREW_CHIEF && (jobChannelId || assigneeChannelId)){
    const inviteChannelId = assigneeChannelId || jobChannelId;
    await ensureBotInChannel(inviteChannelId);
    try {
      const chiefIds = await resolveChiefSlackIds({ assigneeName, deal });
      if (chiefIds.length) await inviteUsersToChannel(inviteChannelId, chiefIds);
    } catch { /* ignore */ }
  }

  // Job channel (respect mode + active deal)
  if (jobChannelId && isDealActive(deal) && shouldPostToJobChannel({ assigneeChannelId })){
    await ensureBotInChannel(jobChannelId);
    try {
      if (JOB_CHANNEL_STYLE === 'summary') {
        const notice = buildJobChannelNotice({ activity, assigneeName });
        await app.client.chat.postMessage({ channel: jobChannelId, text: notice });
      } else if (ENABLE_SLACK_PDF_UPLOAD) {
        await uploadPdfToSlack({ channel: jobChannelId, filename, pdfBuffer, title:`Work Order — ${activity.subject || ''}`, initialComment: summary });
      }
    } catch(e){ /* ignore */ }
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
      } catch(e){ /* ignore */ }
    }
  }

  if (ENABLE_PD_FILE_UPLOAD){ try{ await uploadPdfToPipedrive({ dealId, pdfBuffer, filename }); } catch(e){ console.error('[WO] PD upload failed:', e?.message||e); } }
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

      // de-dup incoming PD webhooks
      if (!DISABLE_EVENT_DEDUP && alreadyHandledEvent(meta, data)) {
        dbgRename('skip-dedup', {
          entity, action, aid: data?.id,
          meta: { id: meta?.entity_id, req: meta?.request_id, ts: meta?.timestamp }
        });
        return res.status(200).send('OK');
      }

      // fetch the latest activity
      const aRes = await fetch(
        `https://api/pipedrive.com/v1/activities/${encodeURIComponent(data.id)}?api_token=${PIPEDRIVE_API_TOKEN}`.replace('api/','api.')
      );
      const aJson = await aRes.json();
      if (!aJson?.success || !aJson.data) return res.status(200).send('Activity fetch fail');
      const activity = aJson.data;

      dbgRename('activity-fetched', {
        id: activity.id, subject: activity.subject, type: activity.type,
        due_date: activity.due_date, due_time: activity.due_time, done: activity.done
      });

      if (activity.done === true || activity.done === 1) return res.status(200).send('OK');

      // fetch the deal (so we can fall back to deal-level enum)
      let deal = null;
      if (activity.deal_id) {
        const dRes = await fetch(
          `https://api/pipedrive.com/v1/deals/${encodeURIComponent(activity.deal_id)}?api_token=${PIPEDRIVE_API_TOKEN}`.replace('api/','api.')
        );
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) deal = dJson.data;
      }
      if (deal) dbgRename('deal-fetched', { id: deal.id, status: deal.status });
      if (deal && !isDealActive(deal)) return res.status(200).send('OK');

      // resolve assignee: activity enum → deal enum (fallback)
      const assignee = detectAssignee({ deal, activity, allowDealFallback: true });
      console.log(
        `[ASSIGNEE/ACT] id=${activity.id} -> ${JSON.stringify({
          teamName: assignee.teamName, teamId: assignee.teamId, channelId: !!assignee.channelId && 'yes'
        })}`
      );
      dbgRename('assignee', assignee);

      // ===== RENAME GATE =====
      if (isTypeAllowedForRename(activity) &&
          !subjectMatchesList(activity.subject, NEVER_RENAME_SUBJECTS) &&
          assignee.teamName) {
        const r = await ensureCrewTagMatches(activity.id, activity.subject || '', assignee.teamName);
        if (r?.did && r.subject) activity.subject = r.subject;
      } else {
        dbgRename('rename-skipped', {
          typeAllowed: isTypeAllowedForRename(activity),
          inNeverList: subjectMatchesList(activity.subject, NEVER_RENAME_SUBJECTS),
          hasTeamName: !!assignee.teamName
        });
      }

      // === Slack posting is gated by due date ===
      if (!(POST_FUTURE_WOS || isDueTodayCT(activity))) {
        try { await deleteAssigneePost(activity.id); } catch {}
        return res.status(200).send('OK');
      }
      if (!shouldPostNowStrong(activity, assignee.teamName, deal)) {
        return res.status(200).send('OK');
      }

      const jobChannelId = await resolveDealChannelId({ allowDefault: ALLOW_DEFAULT_FALLBACK });
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

      // fetch full, current deal
      let deal = null;
      try {
        const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(dealId)}?api_token=${PIPEDRIVE_API_TOKEN}`.replace('api/','api.'));
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) deal = dJson.data;
      } catch (e) {
        console.warn('[PD Hook] DEAL fetch failed; falling back to webhook data', e?.message || e);
      }
      deal = deal || data;

      // list all OPEN activities on this deal
      const listRes = await fetch(
        `https://api.pipedrive.com/v1/activities?deal_id=${encodeURIComponent(dealId)}&done=0&start=0&limit=50&api_token=${PIPEDRIVE_API_TOKEN}`.replace('api/','api.')
      );
      const listJson = await listRes.json();
      const items = (listJson?.data || []).filter(a => a && (a.done === false || a.done === 0));

      const jobChannelId = await resolveDealChannelId({ allowDefault: ALLOW_DEFAULT_FALLBACK });

      for (const activity of items) {
        const ass = detectAssignee({ deal, activity, allowDealFallback: true });

        // ====== SAME RENAME GATE ON DEAL FAN-OUT ======
        try {
          if (isTypeAllowedForRename(activity) &&
              !subjectMatchesList(activity.subject, NEVER_RENAME_SUBJECTS) &&
              ass.teamName) {
            const r = await ensureCrewTagMatches(activity.id, activity.subject || '', ass.teamName);
            if (r?.did && r.subject) activity.subject = r.subject;
          } else {
            dbgRename('deal-update-gates', {
              aid: activity.id,
              typeAllowed: isTypeAllowedForRename(activity),
              inNeverList: subjectMatchesList(activity.subject, NEVER_RENAME_SUBJECTS),
              hasTeamName: !!ass.teamName
            });
          }
        } catch (e) { /* ignore */ }

        // Posting behavior: only due today (unless POST_FUTURE_WOS)
        if (!(POST_FUTURE_WOS || isDueTodayCT(activity))) {
          try { await deleteAssigneePost(activity.id); } catch {}
          continue;
        }

        if (!shouldPostNowStrong(activity, ass.teamName, deal)) continue;

        await postWorkOrderToChannels({
          activity, deal, jobChannelId,
          assigneeChannelId: ass.channelId,
          assigneeName: ass.teamName,
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
    const listRes = await fetch(`https://api.pipedrive.com/v1/activities?done=0&limit=500&api_token=${PIPEDRIVE_API_TOKEN}`.replace('api/','api.'));
    const listJson = await listRes.json();
    const all = Array.isArray(listJson?.data) ? listJson.data : [];
    const dueToday = all.filter(a => (a?.due_date||'').trim() === today);
    dueToday.sort(compareByStartTime);

    let posted = 0;
    for (const activity of dueToday) {
      let deal = null;
      if (activity.deal_id){
        const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(activity.deal_id)}?api_token=${PIPEDRIVE_API_TOKEN}`.replace('api/','api.'));
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) deal = dJson.data;
      }
      if (deal && !isDealActive(deal)) continue;

      const assignee = detectAssignee({ deal, activity, allowDealFallback: true });
      const jobChannelId = await resolveDealChannelId({ allowDefault: ALLOW_DEFAULT_FALLBACK });

      // ====== SAME RENAME GATE IN 7AM RUN ======
      try {
        if (isTypeAllowedForRename(activity) &&
            !subjectMatchesList(activity.subject, NEVER_RENAME_SUBJECTS) &&
            assignee.teamName) {
          const r = await ensureCrewTagMatches(activity.id, activity.subject || '', assignee.teamName);
          if (r?.did && r.subject) activity.subject = r.subject;
        }
      } catch {}

      if (!shouldPostNowStrong(activity, assignee.teamName, deal)) continue;

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
    const pdResp = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(aid)}?api_token=${PIPEDRIVE_API_TOKEN}`.replace('api/','api.'), {
      method:'PUT', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ done:true, marked_as_done_time: markedAtIso })
    });
    const pdJson = await pdResp.json();
    const ok = !!(pdJson && pdJson.success);

    let activity = null, deal = null, dealTitle='N/A', typeOfService='N/A', location='N/A';
    try {
      const aRes = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(aid)}?api_token=${PIPEDRIVE_API_TOKEN}`.replace('api/','api.'));
      const aJson = await aRes.json();
      if (aJson?.success && aJson.data) {
        activity = aJson.data;
        const dealId = did || activity.deal_id;
        if (dealId) {
          const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(dealId)}?api_token=${PIPEDRIVE_API_TOKEN}`.replace('api/','api.'));
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
      const up = await uploadPdfToPipedrive({ dealId: dealIdForUpload, pdfBuffer: completedPdf, filename: `WO_${aid}_Completed.pdf` });
      console.log('[WO] completion PDF uploaded', up?.success);
      const jobChannel = cid || await resolveDealChannelId({ allowDefault: ALLOW_DEFAULT_FALLBACK }) || DEFAULT_CHANNEL;
      if (jobChannel) {
        await uploadPdfToSlack({ channel: jobChannel, filename: `WO_${aid}_Completed.pdf`, pdfBuffer: completedPdf, title: 'Work Order Completed', initialComment: `✅ Completed Work Order for activity ${aid}. ${AID_TAG(aid)}` });
      }
    } catch (e) {
      console.error('[WO] completion PDF upload failed', e?.message||e);
    }

    try {
      const when = new Date().toLocaleString();
      const subject = activity?.subject ? `“${activity.subject}”` : '';
      await fetch(`https://api.pipedrive.com/v1/notes?api_token=${PIPEDRIVE_API_TOKEN}`.replace('api/','api.') ,{
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
    const aRes = await fetch(`https://api.pipedrive.com/v1/activities/${encodeURIComponent(aid)}?api_token=${PIPEDRIVE_API_TOKEN}`.replace('api/','api.'));
    const aj = await aRes.json();
    if (!aj?.success || !aj.data) return res.status(404).send('Activity not found');
    const data = aj.data;

    // No invoice filtering: always allow building WO PDF
    let dealTitle='N/A', typeOfService='N/A', location='N/A', assigneeName=null, deal=null, customerName=null;
    const dealId = data.deal_id;
    if (dealId){
      const dRes = await fetch(`https://api.pipedrive.com/v1/deals/${encodeURIComponent(dealId)}?api_token=${PIPEDRIVE_API_TOKEN}`.replace('api/','api.'));
      const dj = await dRes.json();
      if (dj?.success && dj.data){
        deal = dj.data;
        dealTitle = deal.title || 'N/A';
        const serviceId = readEnumId(deal?.['5b436b45b63857305f9691910b6567351b5517bc']);
        typeOfService = SERVICE_MAP[serviceId] || 'N/A';
        location = await getBestLocation(deal);
        const ass = detectAssignee({ deal, activity: data, allowDealFallback: true });
        assigneeName = ass.teamName || assigneeName;
        const pid = deal?.person_id?.value || deal?.person_id?.id || deal?.person_id;
        if (pid) {
          const pres = await fetch(`https://api.pipedrive.com/v1/persons/${encodeURIComponent(pid)}?api_token=${PIPEDRIVE_API_TOKEN}`.replace('api/','api.'));
          const pjson = await pres.json();
          if (pjson?.success && pjson.data) customerName = pjson.data.name || null;
        }
      }
    }

    const channelIdForQr = await resolveDealChannelId({ allowDefault: ALLOW_DEFAULT_FALLBACK });
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
(async () => {
  await app.start(PORT);
  console.log(`✅ Dispatcher running on port ${PORT}`);
})();
