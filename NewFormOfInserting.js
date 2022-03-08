

const lineReader = require('line-reader');
const env = require("dotenv").config();
const sql = require('mssql')

let counter = 0;
let bulkList = []

const sqlConfig = {
  user: process.env.user,
  password: process.env.password,
  database: process.env.database,
  server: 'localhost',
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  },
  options: {
    encrypt: true, // for azure
    trustServerCertificate: true // change to true for local dev / self-signed certs
  }
}

let countCounter = 0;

const titleType = [
  "tvEpisode",
  "short",
  "tvMoives",
  "tvSeries",
  "tvSpecial",
  "tvMiniSeries",
  "documentary",
  "videoGame",
  "movie",
  "video",
  "tvShort",
  "tvMovie",
];

let table = giveEmptyTable()

lineReader.eachLine('data.tsv', function(line,last) {



  counter++
  let array = line.split("\t");
      for (let index = 0; index < array.length; index++) {
        if (array[index] == "\\N") {
          array[index] = null;
        }
      }

    table.rows.add(
      array[0],
      (titleType.indexOf(array[1]) + 1).toString(),
      array[2],
      array[3],
      array[4],
      array[5],
      array[6],
      array[7]
    );
      
    if (counter==250000 || last==true) {
      insertData(table)
      counter=0
     countCounter++
      table = giveEmptyTable()
      

      if (last==true) {
        console.log('Done')
        return false
      }
    }


});

function giveEmptyTable(){
  let table = new sql.Table('Movies') // or temporary table, e.g. #temptable
    table.create = true
    table.columns.add('tconst', sql.VarChar(255), {nullable: false, primary: true})
    table.columns.add("titleType", sql.VarChar(255), { nullable: false });
    table.columns.add("primaryTitle", sql.NVarChar(sql.MAX), {  nullable: true });
    table.columns.add("OriginalTitle", sql.NVarChar(sql.MAX), { nullable: true });
    table.columns.add("isAdult", sql.VarChar(255), { nullable: true });
    table.columns.add("startYear", sql.VarChar(255), { nullable: true });
    table.columns.add("endYear", sql.VarChar(255), { nullable: true });
    table.columns.add("runtimeMinutes", sql.VarChar(255), { nullable: true });
    
    return table
}

async function insertData(table){
  try {
  
const poolPromise = new sql.ConnectionPool(sqlConfig)
  .connect()
  .then(pool => {
   // console.log('Connected to MSSQL')
    pool.request().bulk(table);
    return pool
  })
  .catch(err => console.log('Database Connection Failed! Bad Config: ', err))



  }catch (error) {
    console.log(error)
    console.log(table)
    return
  }

}