// Dispatcher Bot for Slack – Mike-Only Version
// Posts all open Pipedrive tasks assigned to Mike (Production Team ID 53) to the specified schedule Slack channel

const express = require('express');
const { App } = require('@slack/bolt');
const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN;
const SLACK_SIGNING_SECRET = process.env.SLACK_SIGNING_SECRET;
const PIPEDRIVE_API_TOKEN = process.env.PIPEDRIVE_API_TOKEN;
const SCHEDULE_CHANNEL_ID = 'C098H8GU355';

const bolt = new App({
  token: SLACK_BOT_TOKEN,
  signingSecret: SLACK_SIGNING_SECRET,
});

const PRODUCTION_TEAM_ID = 53; // Mike

async function fetchTasksForMike() {
  const url = `https://api.pipedrive.com/v1/activities?api_token=${PIPEDRIVE_API_TOKEN}&user_id=53&done=0`;
  const res = await fetch(url);
  const json = await res.json();
  return json.data || [];
}

function formatTaskMessage(task) {
  return `📌 *${task.subject}*
👤 Contact: ${task.person_name || 'N/A'}
🔧 Type of Service: ${task.deal?.type_of_service || 'N/A'}
🔥 Priority: ${task.priority || 'Normal'}
📆 Due: ${task.due_date} ${task.due_time || ''}
🔗 Deal ID: ${task.deal_id}`;
}

async function postScheduleToSlack() {
  const tasks = await fetchTasksForMike();
  if (!tasks.length) return;

  for (const task of tasks) {
    const message = formatTaskMessage(task);
    await bolt.client.chat.postMessage({
      channel: SCHEDULE_CHANNEL_ID,
      text: message,
    });
  }
}

bolt.command('/dispatch', async ({ ack, respond }) => {
  await ack();
  await postScheduleToSlack();
  await respond('✅ Schedule posted for Mike.');
});

(async () => {
  await bolt.start(port);
  console.log(`Dispatcher (Mike Test) running on port ${port}`);
})();
