// routes/upload.js
const express = require('express');
const router = express.Router();
const multer = require('multer');              // npm i multer
const upload = multer();                       // memory storage
const { parse } = require('csv-parse/sync');   // npm i csv-parse@5

const {
  CLIENT_KEYMAP, TX_KEYMAP,
  normalizeRow, groupByClient, scoreClient
} = require('../lib/riskEngine');

// POST /api/upload  (expects 2 file fields: "clients" and "transactions")
router.post(
  '/upload',
  upload.fields([{ name: 'clients', maxCount: 1 }, { name: 'transactions', maxCount: 1 }]),
  (req, res) => {
    try {
      const clientsBuf = req.files?.clients?.[0]?.buffer;
      const txBuf      = req.files?.transactions?.[0]?.buffer;
      if (!clientsBuf || !txBuf) {
        return res.status(400).json({ ok: false, error: 'Missing files. Expect fields: clients, transactions' });
      }

      const clientsRaw = parse(clientsBuf.toString('utf8'), { columns: true, skip_empty_lines: true });
      const txRaw      = parse(txBuf.toString('utf8'),      { columns: true, skip_empty_lines: true });

      const clients = clientsRaw.map(r => normalizeRow(r, CLIENT_KEYMAP));
      const tx      = txRaw.map(r => normalizeRow(r, TX_KEYMAP));
      const by      = groupByClient(tx);

      // DEBUG: shows normalized keys in Render logs so you can confirm headers matched
      console.log('Sample client keys:', Object.keys(clients[0] || {}));

      const results = clients.map(c => {
        const t = by.get(c.ClientID) || [];
        const r = scoreClient(c, t, new Date());
        return {
          ClientID: c.ClientID,
          Name: c.Name,
          Band: r.band,
          Score: r.score,
          Reasons: r.reasons
        };
      }).sort((a, b) => b.Score - a.Score);

      return res.json({ ok: true, count: results.length, results });
    } catch (e) {
      console.error(e);
      return res.status(500).json({ ok: false, error: String(e) });
    }
  }
);

module.exports = router;
