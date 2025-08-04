const express = require('express');
const bodyParser = require('body-parser');
const fetch = require('node-fetch');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN;
const DEFAULT_CHANNEL = '#dispatcher-feed'; // Change to actual channel name or pass dynamically

app.use(bodyParser.json());

// Health check route
app.get('/', (req, res) => {
  res.send('🚀 Dispatcher is running.');
});

// Trigger work order route
app.post('/trigger-workorder', async (req, res) => {
  const { dealTitle, productionTeam, taskType, dueDate, jobId } = req.body;

  if (!dealTitle || !productionTeam || !taskType) {
    return res.status(400).send('❌ Missing required fields: dealTitle, productionTeam, taskType');
  }

  const message = {
    channel: DEFAULT_CHANNEL,
    text: `📋 *New Work Order Created*\n*${taskType}* - *${dealTitle}* - *${productionTeam}*\n🗓️ Due: ${dueDate || 'Unspecified'}${jobId ? `\n🔗 Job ID: ${jobId}` : ''}`
  };

  try {
    const slackRes = await fetch('https://slack.com/api/chat.postMessage', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${SLACK_BOT_TOKEN}`
      },
      body: JSON.stringify(message)
    });

    const slackJson = await slackRes.json();

    if (!slackJson.ok) {
      console.error('Slack API Error:', slackJson);
      return res.status(500).send(`Slack error: ${slackJson.error}`);
    }

    console.log(`✅ Work order posted to ${DEFAULT_CHANNEL}`);
    res.status(200).send('✅ Work order posted.');
  } catch (err) {
    console.error('❌ Server Error:', err);
    res.status(500).send('Internal server error.');
  }
});

app.listen(PORT, () => {
  console.log(`🟢 Dispatcher server listening on port ${PORT}`);
});
