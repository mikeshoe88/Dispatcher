import dotenv from 'dotenv';
dotenv.config();

import fetch from 'node-fetch';
import bolt from '@slack/bolt';
const { App, ExpressReceiver } = bolt;
import express from 'express';

// 🔧 Constants
const PORT = process.env.PORT || 3000;
const MIKE_SLACK_ID = 'U05FPCPHJG6';
const SCHEDULE_CHANNEL = 'C098H8GU355';
const PIPEDRIVE_API_TOKEN = process.env.PIPEDRIVE_API_TOKEN;

// 🔧 Slack App Init
const receiver = new ExpressReceiver({
  signingSecret: process.env.SLACK_SIGNING_SECRET,
  endpoints: '/slack/events',
  processBeforeResponse: true,
  bodyParser: false
});

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  signingSecret: process.env.SLACK_SIGNING_SECRET,
  receiver
});

// ✅ Respond to @ mentions
app.event('app_mention', async ({ event, say }) => {
  await say(`Hey <@${event.user}>, Dispatcher is online and running!`);
});

// 🧾 Handle Webhook Event from Pipedrive
const expressApp = receiver.app;

expressApp.post('/pipedrive-task', express.urlencoded({ extended: true }), async (req, res) => {
  console.log('📥 Received request at /pipedrive-task');

  try {
    console.log('✅ Incoming Pipedrive Payload:', req.body);
    const payload = req.body;
    const activity = payload.current;

    if (!activity) {
      console.log('⚠️ No activity object found.');
      return res.status(200).send('No activity object.');
    }

    if (activity.assigned_to_user_id != 53) {
      console.log(`🔁 Task assigned to someone else: ${activity.assigned_to_user_id}`);
      return res.status(200).send('Not for Mike.');
    }

    const message = `📌 *New Task Created for Mike*
• *${activity.subject}*
📅 Due: ${activity.due_date || 'No due date'}
🔗 Deal: ${activity.deal_title || 'N/A'} | Org: ${activity.org_name || 'N/A'}`;

    console.log('📤 Sending message to Slack...');
    await app.client.chat.postMessage({
      channel: SCHEDULE_CHANNEL,
      text: message
    });

    console.log('✅ Message posted to Slack');
    res.status(200).send('Task processed.');
  } catch (error) {
    console.error('❌ Error processing webhook:', error);
    res.status(500).send('Server error.');
  }
});

// 🚀 Start unified Bolt + Express Server
(async () => {
  await app.start(PORT);
  console.log(`✅ Dispatcher (Mike Test) running on port ${PORT}`);
})();
