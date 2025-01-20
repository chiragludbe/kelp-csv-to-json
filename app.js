const express = require('express')
const fs = require('fs') 
const app = express()
const dotenv = require('dotenv')
dotenv.config()

const port = process.env.PORT

app.use(express.json());


// Optimized utility function for large files and deep properties
function parseLargeCsv(csvData) {
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
  
    // return result;
    return processResult(result);
}

function processResult(resultData) {
    const processedData = [];
    resultData.map((data)=> {
        let tuple = [];
        if(data['name']) {
            tuple.push(data['name']['firstName'] + ' ' + data['name']['lastName']);
            delete data['name'];
        }

        if(data['address']) {
            tuple.push(data['address']);
            delete data['address'];
        }

        if(data['age']){
            tuple.push(data['age']);
            delete data['age'];
        }
        tuple.push(data);
        processedData.push(tuple);
    })
    return processedData;
}
  

app.get('/', (req, res) => {
  res.send('Hello World!')
})

// API route to convert CSV to JSON
app.get('/convert', (req, res) => {
    try {
        //TODO: add path in env file
        const fileBuffer = fs.readFileSync(process.env.CSV_PATH)
        // console.log(fileBuffer.toString());

        // Read and parse CSV
        const csv = fileBuffer.toString();
  
        if (!csv) {
            return res.status(400).json({ error: "CSV data is required in the request body" });
        }
    
        const jsonData = parseLargeCsv(csv);
        res.json(jsonData);
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
});

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
});