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
expressApp.use(express.json());

expressApp.post('/pipedrive-task', async (req, res) => {
  console.log('📥 Received request at /pipedrive-task');

  try {
    const payload = req.body;
    console.log('✅ Incoming Pipedrive Payload:', JSON.stringify(payload, null, 2));

    const activity = payload.data;
    if (!activity) {
      console.log('⚠️ No activity object found.');
      return res.status(200).send('No activity object.');
    }

    if (activity.owner_id !== 23457092) {
      console.log(`🔁 Task assigned to someone else: ${activity.owner_id}`);
      return res.status(200).send('Not for Mike.');
    }

    // Fetch full activity details including the note body
    const activityDetailsRes = await fetch(`https://api.pipedrive.com/v1/activities/${activity.id}?api_token=${PIPEDRIVE_API_TOKEN}`);
    const activityDetails = await activityDetailsRes.json();

    let fullNote = '_No note provided_';
    let dealId = 'N/A';
    let dealTitle = 'N/A';
    let typeOfService = 'N/A';
    let location = 'N/A';

    if (activityDetails.success && activityDetails.data) {
      const data = activityDetails.data;
      fullNote = data.note || fullNote;
      dealId = data.deal_id || 'N/A';

      // Fetch deal info
      if (dealId && dealId !== 'N/A') {
        const dealRes = await fetch(`https://api.pipedrive.com/v1/deals/${dealId}?api_token=${PIPEDRIVE_API_TOKEN}`);
        const dealInfo = await dealRes.json();
        if (dealInfo.success && dealInfo.data) {
          dealTitle = dealInfo.data.title || 'N/A';
          typeOfService = dealInfo.data['5b436b45b63857305f9691910b6567351b5517bc'] || 'N/A';
          location = dealInfo.data.location || 'N/A';
        }
      }
    }

    const message = `📌 *New Task Created for Mike*
• *${activity.subject}*
📅 Due: ${activity.due_date || 'No due date'}
📝 Note: ${fullNote}
🏷️ Deal ID: ${dealId} - *${dealTitle}*
📦 Type of Service: ${typeOfService}
📍 Location: ${location}
✅ _Click the checkbox above to complete_`;

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
