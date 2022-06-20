import * as pgwire from "pgwire";
import * as common from "./common.js";
import * as dotenv from "dotenv";

dotenv.config();

const args = process.argv.slice(2);

const conn = await pgwire.pgconnect(
  {
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
    hostname: process.env.PG_HOST,
    port: process.env.PG_PORT,
    database: process.env.PG_DATABASE,
  },
  {
    replication: "database",
  }
);
let organisationUnits = {};
try {
  // const [stopLsn] = await conn.query(`select pg_current_wal_lsn()`);
  // if (args[0] !== "organisationunit") {
  //   const data = await common.api.post(`wal/index?index=${channel}`, data);
  // }
  const replicationStream = conn.logicalReplication({
    slot: `${args[0]}_slot`,
    startLsn: "0/0",
    options: {
      proto_version: 1,
      publication_names: `${args[0]}_pub`,
      // messages: "true",
    },
  });
  for await (const chunk of replicationStream.pgoutputDecode()) {
    const { messages, lastLsn } = chunk;
    const data = messages
      .filter(({ tag }) => ["update", "insert"].indexOf(tag) !== -1)
      .map(({ after }) => {
        const { geometry, ...others } = after;
        return others;
      });
    try {
      if (data.length > 0) {
        await common.api.post(`wal/index?index=${args[0]}`, {
          data,
          index: args[0],
        });
      }
      // replicationStream.ack(lastLsn);
    } catch (error) {
      console.log(error.message);
    }
    // if (lastLsn >= stopLsn) {
    //   break;
    // }
  }
} finally {
  conn.end();
}
