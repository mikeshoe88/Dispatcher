const express = require("express");
const bodyParser = require("body-parser");
const axios = require("axios");
const PDFDocument = require("pdfkit");
const { App } = require("@slack/bolt");

const PORT = process.env.PORT || 8080;
const API_TOKEN = process.env.API_TOKEN;
const PD_SECRET = process.env.PD_WEBHOOK_KEY;
const PRODUCTION_TEAM_FIELD_KEY = process.env.PRODUCTION_TEAM_FIELD_KEY;
const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN;
const SLACK_SIGNING_SECRET = process.env.SLACK_SIGNING_SECRET;

// Hard-coded channel for now (your schedule channel)
const SCHEDULE_CHANNEL = process.env.DEFAULT_SLACK_CHANNEL_ID || "C098H8GU355";

// Slack app init
const app = new App({
  token: SLACK_BOT_TOKEN,
  signingSecret: SLACK_SIGNING_SECRET
});

// Express app
const expressApp = express();
expressApp.use(bodyParser.json());

// --- PIPEDRIVE TASK WEBHOOK ---
expressApp.post("/pipedrive-task", async (req, res) => {
  try {
    // Verify secret
    const key = req.query.key;
    if (key !== PD_SECRET) {
      console.warn("Invalid PD secret");
      return res.status(403).send("Forbidden");
    }

    const body = req.body;
    console.log("Webhook body:", JSON.stringify(body, null, 2));

    if (!body?.current) {
      return res.status(200).send("No current payload");
    }

    const task = body.current;
    const dealId = task.deal_id;
    const subject = task.subject || "New Task";
    const note = task.note || "";
    const due = task.due_date || "";

    // --- Create PDF buffer ---
    const doc = new PDFDocument({ margin: 36 });
    const chunks = [];
    doc.on("data", (c) => chunks.push(c));
    const done = new Promise((resolve) => doc.on("end", () => resolve(Buffer.concat(chunks))));
    doc.fontSize(20).text("Work Order", { align: "center" });
    doc.moveDown();
    doc.fontSize(12).text(`Deal ID: ${dealId}`);
    doc.text(`Task: ${subject}`);
    doc.text(`Due: ${due}`);
    doc.moveDown().text("Notes:");
    doc.text(note);
    doc.end();
    const pdfBuffer = await done;

    // --- Post to Slack ---
    await app.client.chat.postMessage({
      channel: SCHEDULE_CHANNEL,
      text: `📌 *New Task*: ${subject}\nDue: ${due}\nDeal: ${dealId}`
    });

    await app.client.files.upload({
      channels: SCHEDULE_CHANNEL,
      filename: `workorder-${dealId}.pdf`,
      file: pdfBuffer,
      title: `Work Order for Deal ${dealId}`,
      initial_comment: "Attached Work Order PDF"
    });

    res.status(200).send("ok");
  } catch (e) {
    console.error("Error in /pipedrive-task:", e.response?.data || e);
    res.status(500).send("error");
  }
});

// --- DEBUG: render PDF directly in browser ---
expressApp.get("/debug/pdf", async (_req, res) => {
  try {
    const doc = new PDFDocument({ margin: 36 });
    const chunks = [];
    doc.on("data", (c) => chunks.push(c));
    doc.on("end", () => {
      const buf = Buffer.concat(chunks);
      res.setHeader("Content-Type", "application/pdf");
      res.setHeader("Content-Length", buf.length);
      res.send(buf);
    });
    doc.fontSize(20).text("Dispatcher PDF Test", { align: "center" });
    doc.moveDown();
    doc.fontSize(12).text("If you can read this, pdfkit is working on Railway.");
    doc.end();
  } catch (e) {
    console.error("DEBUG /pdf error:", e);
    res.status(500).send("error");
  }
});

// --- DEBUG: upload PDF to Slack ---
expressApp.get("/debug/upload-test", async (req, res) => {
  try {
    const channel = req.query.cid || SCHEDULE_CHANNEL;

    const doc = new PDFDocument({ margin: 36 });
    const chunks = [];
    doc.on("data", (c) => chunks.push(c));
    const finished = new Promise((r) => doc.on("end", () => r(Buffer.concat(chunks))));
    doc.fontSize(18).text("Dispatcher Slack Upload Test", { align: "center" });
    doc.moveDown().fontSize(12).text(`Channel: ${channel}`);
    doc.end();
    const pdfBuffer = await finished;

    const result = await app.client.files.upload({
      channels: channel,
      filename: "dispatcher-test.pdf",
      file: pdfBuffer,
      title: "Dispatcher Test PDF",
      initial_comment: "Test upload from /debug/upload-test"
    });

    console.log("DEBUG upload result:", result.ok);
    res.status(200).send("Uploaded test PDF to Slack.");
  } catch (e) {
    console.error("DEBUG /upload-test error:", e.data || e);
    res.status(500).send(`upload failed: ${e.data ? JSON.stringify(e.data) : e.message}`);
  }
});

// Start Bolt + Express
(async () => {
  await app.start(PORT);
  expressApp.listen(PORT, () => {
    console.log(`🚀 Dispatcher running on port ${PORT}`);
  });
})();
