// index.js
import dotenv from 'dotenv';
dotenv.config();

import fetch from 'node-fetch';
import bolt from '@slack/bolt';
const { App, ExpressReceiver } = bolt;
import express from 'express';
import crypto from 'crypto';

// ENV / constants
const PORT = process.env.PORT || 3000;
const PIPEDRIVE_API_TOKEN = process.env.PIPEDRIVE_API_TOKEN;
const SIGNING_SECRET = process.env.WO_QR_SECRET; // HMAC secret for QR links
const SCHEDULE_CHANNEL = process.env.DEFAULT_SLACK_CHANNEL_ID || 'C098H8GU355';

if (!process.env.SLACK_SIGNING_SECRET) throw new Error('Missing SLACK_SIGNING_SECRET');
if (!process.env.SLACK_BOT_TOKEN) throw new Error('Missing SLACK_BOT_TOKEN');
if (!PIPEDRIVE_API_TOKEN) throw new Error('Missing PIPEDRIVE_API_TOKEN');
if (!SIGNING_SECRET) throw new Error('Missing WO_QR_SECRET');

// Optional: map Pipedrive service enum -> label
const SERVICE_MAP = {
  27: 'Water Mitigation',
  28: 'Fire Cleanup',
  29: 'Contents',
  30: 'Biohazard',
  31: 'General Cleaning',
  32: 'Duct Cleaning',
};

// Slack app init
const receiver = new ExpressReceiver({
  signingSecret: process.env.SLACK_SIGNING_SECRET,
  endpoints: {
    events: '/slack/events',
    commands: '/slack/commands',
    actions: '/slack/interact',
  },
  processBeforeResponse: true,
});

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  signingSecret: process.env.SLACK_SIGNING_SECRET,
  receiver,
});

// Ping
app.event('app_mention', async ({ event, say }) => {
  await say(`Hey <@${event.user}>, Dispatcher is online and running!`);
});

// Slack checkbox → complete PD activity
app.action('complete_task', async ({ body, ack, client }) => {
  await ack();
  const checkboxValue = body.actions?.[0]?.selected_options?.[0]?.value;
  const activityId = checkboxValue?.replace('task_', '');
  if (!activityId) return;

  try {
    const pdUrl = `https://api.pipedrive.com/v1/activities/${activityId}?api_token=${PIPEDRIVE_API_TOKEN}`;
    const resp = await fetch(pdUrl, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ done: true, marked_as_done_time: new Date().toISOString() }),
    });
    const j = await resp.json();
    console.log('PD complete result:', j);
  } catch (e) {
    console.error('PD complete error:', e);
  }

  try {
    await client.chat.delete({ channel: body.channel.id, ts: body.message.ts });
  } catch (e) {
    console.error('Slack delete error:', e);
  }
});

// Express app from Bolt receiver
const expressApp = receiver.app;
expressApp.use(express.json());

// Pipedrive webhook → post task to Slack
expressApp.post('/pipedrive-task', async (req, res) => {
  try {
    const activity = req.body?.data;
    if (!activity) return res.status(200).send('No activity.');

    // Get full activity info
    const aRes = await fetch(
      `https://api.pipedrive.com/v1/activities/${activity.id}?api_token=${PIPEDRIVE_API_TOKEN}`
    );
    const aJson = await aRes.json();

    let fullNote = '_No note provided_';
    let dealId = 'N/A';
    let dealTitle = 'N/A';
    let typeOfService = 'N/A';
    let location = 'N/A';

    if (aJson?.success && aJson.data) {
      const data = aJson.data;
      fullNote = (data.note || fullNote).replace(/<br\/?>(\s)?/g, '\n').replace(/&nbsp;/g, ' ').trim();
      dealId = data.deal_id || 'N/A';

      if (dealId && dealId !== 'N/A') {
        const dRes = await fetch(
          `https://api.pipedrive.com/v1/deals/${dealId}?api_token=${PIPEDRIVE_API_TOKEN}`
        );
        const dJson = await dRes.json();
        if (dJson?.success && dJson.data) {
          dealTitle = dJson.data.title || 'N/A';
          const serviceId = dJson.data['5b436b45b63857305f9691910b6567351b5517bc'];
          typeOfService = SERVICE_MAP[serviceId] || serviceId || 'N/A';
          location = dJson.data.location || 'N/A';
        }
      }
    }

    const message = {
      channel: SCHEDULE_CHANNEL,
      text: `📌 *New Task*\n• *${activity.subject}*\n🗕️ Due: ${activity.due_date || 'No due date'}\n📜 Note: ${fullNote}\n🏷️ Deal ID: ${dealId} - *${dealTitle}*\n📦 Type of Service: ${typeOfService}\n📍 Location: ${location}\n✅ _Click the checkbox below to complete_`,
      blocks: [
        {
          type: 'section',
          text: {
            type: 'mrkdwn',
            text: `📌 *New Task*\n• *${activity.subject}*\n🗕️ Due: ${activity.due_date || 'No due date'}\n📜 Note: ${fullNote}\n🏷️ Deal ID: ${dealId} - *${dealTitle}*\n📦 Type of Service: ${typeOfService}\n📍 Location: ${location}`,
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
    res.status(200).send('OK');
  } catch (error) {
    console.error('Webhook error:', error);
    res.status(500).send('Server error.');
  }
});

// HMAC helpers (base64url)
const b64url = (buf) =>
  Buffer.from(buf).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/,'');

const sign = (raw) => b64url(crypto.createHmac('sha256', SIGNING_SECRET).update(raw).digest());
function verify(raw, sig) {
  try { return crypto.timingSafeEqual(Buffer.from(sign(raw)), Buffer.from(String(sig))); }
  catch { return false; }
}

// QR scan → complete task in PD (+ optional Slack ping)
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

    const channel = cid || SCHEDULE_CHANNEL;
    if (channel) {
      const text = ok
        ? `✅ Task *${aid}* marked complete in Pipedrive${did ? ` (deal ${did})` : ''}.`
        : `⚠️ Tried to complete task *${aid}* but Pipedrive didn’t confirm success.`;
      await app.client.chat.postMessage({ channel, text });
    }

    return res
      .status(200)
      .send(`<html><body style="font-family:Arial;padding:24px">
        <h2>Work Order Complete</h2>
        <p>Task <b>${aid}</b> ${ok ? 'has been updated' : 'could not be updated'} in Pipedrive.</p>
        ${did ? `<p>Deal: <b>${did}</b></p>` : ''}
        <p>${ok ? 'You’re good to go. ✅' : 'Please contact the office. ⚠️'}</p>
      </body></html>`);
  } catch (err) {
    console.error('/wo/complete error:', err);
    return res.status(500).send('Server error.');
  }
});

// Start
(async () => {
  await app.start(PORT); // Bolt spins up the HTTP server on PORT
  console.log(`✅ Dispatcher running on port ${PORT}`);
})();
