const express = require("express");
const fs = require("fs");
const app = express();
const dotenv = require("dotenv");
dotenv.config();

app.use(express.json());

const convertController = require("./controllers/convertController");

// API route to test API
app.get("/", (req, res) => {
  res.send("Convert to JSON API is Working as Expected");
});

// API route to convert CSV to JSON
app.get("/convert", async (req, res) => {
  try {
    const fileBuffer = fs.readFileSync(process.env.CSV_PATH);
    const csv = fileBuffer.toString();

    if (!csv) {
      return res
        .status(400)
        .json({ error: "CSV data is required in the request body" });
    }

    await convertController.parseLargeCsv(csv);
    res.json({ message: "Data processed and stored successfully" });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.listen(process.env.PORT, () => {
  console.log(`Example app listening on port ${process.env.PORT}`);
});
