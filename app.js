const express = require('express')
const fs = require('fs') 
const app = express()
const dotenv = require('dotenv')
dotenv.config()

const port = process.env.PORT

app.use(express.json());
const connection = require('./db-connector');


// Optimized utility function for large files and deep properties
async function parseLargeCsv(csvData) {
    const lines = csvData.split('\n');
    const headers = lines[0].split(',');
  
    if (!headers.includes('name.firstName') || !headers.includes('name.lastName') || !headers.includes('age')) {
      throw new Error("Missing mandatory fields: name.firstName, name.lastName, or age");
    }
  
    const result = new Array(lines.length - 1);
  
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue; // Skip empty lines
  
      const values = line.split(',');
      const obj = {};
  
      headers.forEach((header, index) => {
        const keys = header.split('.');
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

    await processResult(result);
}
const ageGroupCount = {
  '<20': 0,
  '20-40': 0,
  '40-60': 0,
  '>60': 0
}
function classifyAgeGroup(age){
  if(age < 20){
    ageGroupCount['<20']++;
  } else if(age < 40) {
    ageGroupCount['20-40']++;
  } else if(age < 60){
    ageGroupCount['40-60']++;
  } else {
    ageGroupCount['>60']++;
  }
}

async function processResult(resultData) {
  let recordCount = 0;
  
  for(const data of resultData) {
    let tuple = [];
    if(data['name']) {
        tuple.push(data['name']['firstName'] + ' ' + data['name']['lastName']);
        delete data['name'];
    }

    if(data['address']) {
        tuple.push(data['address']);
        delete data['address'];
    }

    if(data['age']) {
        tuple.push(data['age']);
        delete data['age'];
    }
    tuple.push(data);
    try {
      await connection.query(
        `INSERT INTO users (name, age, address, additional_info) VALUES ($1, $2, $3, $4)`,
        [tuple[0], tuple[2], tuple[1], data]);
        recordCount++;
        classifyAgeGroup(tuple[2]);
    } catch (error) {
      console.error('Error inserting data into PostgreSQL:', error);
    }
  }

  const printData = [
    {'Age-Group': '<20', '% Distribution': Math.floor((ageGroupCount['<20']/recordCount)*100)},
    {'Age-Group': '20-40', '% Distribution': Math.floor((ageGroupCount['20-40']/recordCount)*100)},
    {'Age-Group': '40-60', '% Distribution': Math.floor((ageGroupCount['40-60']/recordCount)*100)},
    {'Age-Group': '>60', '% Distribution': Math.floor((ageGroupCount['>60']/recordCount)*100)}
  ];

  console.table(printData);
}
  

app.get('/', (req, res) => {
  res.send('Hello World!')
})

// API route to convert CSV to JSON
app.get('/convert', async (req, res) => {
    try {
        const fileBuffer = fs.readFileSync(process.env.CSV_PATH)

        // Read and parse CSV
        const csv = fileBuffer.toString();
  
        if (!csv) {
            return res.status(400).json({ error: "CSV data is required in the request body" });
        }
    
        await parseLargeCsv(csv);
        res.json({ message: 'Data processed and stored successfully' });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
});

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
});