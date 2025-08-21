// src/utils/csv.js
const fs = require('fs');
const csv = require('csv-parser');

function loadCSV(filePath, handleRow) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', handleRow)
      .on('end', resolve)
      .on('error', reject);
  });
}

module.exports = { loadCSV };