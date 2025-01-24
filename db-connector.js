const { Pool } = require("pg");
let connection;
const connectionPool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});


async function createConnection () {
  try {
    connection = await connectionPool.connect();
  } catch (error) {
    console.error("Error connecting to PostgreSQL:", err)
    throw new Error("Error connecting to PostgreSQL:", err);
  }
  console.log("Connected to PostgreSQL");
  return connection;
}

function releaseConnection() {
  connection.release();
  console.log("Disconnected from PostgreSQL");
}

module.exports = { createConnection, releaseConnection };
