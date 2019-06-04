const _ = require('lodash');
const fs = require('fs');
const parse = require('csv-parse');
const mongoClient = require('mongodb').MongoClient;

if (process.argv.length != 5){
  console.log("Usage: sudo node load_data.js <database> <collection> <filePath>");
  process.exit(-1);
}

const _database = process.argv[2];
const _collection = process.argv[3];
const _filePath = process.argv[4];

const mongodbServerUrl = "mongodb://localhost:27017";

const csvData=[];
fs.createReadStream(_filePath)
  .pipe(parse({delimiter: ','}))
  .on('data', function(csvrow) {
      csvData.push(csvrow);        
  })
  .on('end',function() {
    console.log("csvData length: ", csvData.length);

    const header = csvData[0];

    // Connect to the db
    mongoClient.connect(mongodbServerUrl, function (err, client) {
 
      if(err) throw err;
      
      let db = client.db(_database);

      db.collection(_collection, function (err, collection) {
         
        for (let row of csvData.slice(1)){
          collection.insert(getRowJson(header, row));
        }
      });
          
      db.collection(_collection).count(function (err, count) {
        if (err) throw err;
              
        console.log('Total Rows: ' + count);
      });

      client.close();
                  
    });

  });

function getRowJson(header, row) {
  console.log("ROW: ", row);
  return _.zipObject(header, row);  
}
