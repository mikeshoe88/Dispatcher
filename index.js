const { App } = require('@slack/bolt');
const express = require('express');
const bodyParser = require('body-parser');
require('dotenv').config();

// 🔧 Constants
const PORT = process.env.PORT || 3000;
const MIKE_SLACK_ID = 'U05FPCPHJG6';
const SCHEDULE_CHANNEL = 'C098H8GU355';
const PIPEDRIVE_API_TOKEN = process.env.PIPEDRIVE_API_TOKEN;

// 🔧 Slack App Init
const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  signingSecret: process.env.SLACK_SIGNING_SECRET,
  socketMode: false,
  appToken: process.env.SLACK_APP_TOKEN
});

// ✅ Respond to @ mentions
app.event('app_mention', async ({ event, say }) => {
  await say(`Hey <@${event.user}>, Dispatcher is online and running!`);
});

// 🔁 Fetch Mike's open tasks from Pipedrive
async function fetchMikeTasks() {
  const url = `https://api.pipedrive.com/v1/activities?user_id=53&done=0&api_token=${PIPEDRIVE_API_TOKEN}`;
  const res = await fetch(url);
  const data = await res.json();
  return data.data || [];
}

// 🧱 Format task for Slack
function formatTask(task) {
  const dueDate = task.due_date ? `📅 Due: ${task.due_date}` : '📅 No due date';
  return `• *${task.subject}* (${task.type})
${dueDate} | Deal: ${task.deal_title || 'N/A'} | Org: ${task.org_name || 'N/A'}`;
}

// 📬 Post to Slack
async function postTasksToSlack(tasks) {
  if (!tasks.length) return;
  const blocks = tasks.map(task => ({ type: 'section', text: { type: 'mrkdwn', text: formatTask(task) } }));
  await app.client.chat.postMessage({
    channel: SCHEDULE_CHANNEL,
    text: `Tasks for <@${MIKE_SLACK_ID}>`,
    blocks
  });
}

// 🌐 Webhook Handler
const expressApp = express();
expressApp.use(bodyParser.json());

expressApp.post('/pipedrive-task', async (req, res) => {
  const payload = req.body;
  const task = payload.current;

  try {
    if (task.assigned_to_user_id === 53) {
      const slackText = formatTask(task);
      await app.client.chat.postMessage({
        channel: SCHEDULE_CHANNEL,
        text: `🆕 New Task for <@${MIKE_SLACK_ID}>:\n${slackText}`
      });
    }
    res.status(200).send('Received');
  } catch (err) {
    console.error('❌ Error handling webhook:', err);
    res.status(500).send('Error');
  }
});

// 🚀 Start both Slack and Express
(async () => {
  await app.start(); // Internal Bolt setup, not port binding
  expressApp.listen(PORT, () => {
    console.log(`✅ Dispatcher (Mike Test) running on port ${PORT}`);
  });

  // 📦 Initial task load on boot
  const tasks = await fetchMikeTasks();
  await postTasksToSlack(tasks);
})();
