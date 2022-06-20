const { Pool } = require("pg");
const Cursor = require("pg-cursor");
const { processAndInsert, batchSize, backlogQuery } = require("./common.js");
const dotenv = require("dotenv");
const cron = require("node-cron");
dotenv.config();

const pool = new Pool({
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
});

cron.schedule("*/5 * * * *", async () => {
  const client = await pool.connect();
  try {
    const cursor = client.query(new Cursor(backlogQuery));
    let rows = await cursor.read(batchSize);
    if (rows.length > 0) {
      await processAndInsert("programstageinstance", rows);
    }
    while (rows.length > 0) {
      rows = await cursor.read(batchSize);
      if (rows.length > 0) {
        await processAndInsert("programstageinstance", rows);
      }
    }
  } catch (error) {
    console.log(error.message);
  } finally {
    client.release();
  }
});
