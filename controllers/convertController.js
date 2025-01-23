const connection = require("../db-connector");
let totalRecord = 0;
const ageGroupCount = {
  "<20": 0,
  "20-40": 0,
  "40-60": 0,
  ">60": 0,
};

async function parseLargeCsv(csvData) {
  const lines = csvData.split("\n");
  const headers = lines[0].split(",");

  if (
    !headers.includes("name.firstName") ||
    !headers.includes("name.lastName") ||
    !headers.includes("age")
  ) {
    throw new Error(
      "Missing mandatory fields: name.firstName, name.lastName, or age"
    );
  }

  const result = new Array(lines.length - 1);

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const values = line.split(",");
    const obj = {};

    headers.forEach((header, index) => {
      const keys = header.split(".");
      let current = obj;

      for (let j = 0; j < keys.length; j++) {
        const key = keys[j];
        if (j === keys.length - 1) {
          current[key] = values[index];
        } else {
          current[key] = current[key] || {};
          current = current[key];
        }
      }
    });

    result[i - 1] = obj;
  }

  await processAndUploadResult(result);
}

function classifyAgeGroup(age) {
  if (age < 20) {
    ageGroupCount["<20"]++;
  } else if (age < 40) {
    ageGroupCount["20-40"]++;
  } else if (age < 60) {
    ageGroupCount["40-60"]++;
  } else {
    ageGroupCount[">60"]++;
  }
}

async function processAndUploadResult(resultData) {
  for (const data of resultData) {
    let tuple = [];
    if (data["name"]) {
      tuple.push(data["name"]["firstName"] + " " + data["name"]["lastName"]);
      delete data["name"];
    }

    if (data["address"]) {
      tuple.push(data["address"]);
      delete data["address"];
    }

    if (data["age"]) {
      tuple.push(data["age"]);
      delete data["age"];
    }

    try {
      await connection.query(
        `INSERT INTO users (name, age, address, additional_info) VALUES ($1, $2, $3, $4)`,
        [tuple[0], tuple[2], tuple[1], data]
      );
      totalRecord++;
      classifyAgeGroup(tuple[2]);
    } catch (error) {
      throw new Error("Error inserting data into PostgreSQL:", error);
    }
  }

  const printData = [
    {
      "Age-Group": "<20",
      "% Distribution": getPercentageDistribution(ageGroupCount["<20"]),
    },
    {
      "Age-Group": "20-40",
      "% Distribution": getPercentageDistribution(ageGroupCount["20-40"]),
    },
    {
      "Age-Group": "40-60",
      "% Distribution": getPercentageDistribution(ageGroupCount["40-60"]),
    },
    {
      "Age-Group": ">60",
      "% Distribution": getPercentageDistribution(ageGroupCount[">60"]),
    },
  ];

  console.table(printData);
}

function getPercentageDistribution(count) {
  return Math.floor((count / totalRecord) * 100);
}

module.exports = { parseLargeCsv };
