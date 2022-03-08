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

let table = giveEmptyTable()

lineReader.eachLine('episode.tsv', function(line,last) {

  if ((countCounter == 0) & (counter == 0)) {
    console.log("First line is " + line);
    line = "tt0000000	tt0000000	0	0";
    console.log("First line is " + line);
  }




  counter++
  let array = line.split("\t");
      for (let index = 0; index < array.length; index++) {
        if (array[index] == "\\N") {
          array[index] = null;
        }
      }

    table.rows.add(
      array[0],
      array[1],
      parseInt(array[2]),
      parseInt(array[3]),
    );
      
    if (counter==250000 || last==true) {
      insertData(table)
      counter=0
     countCounter++
      table = giveEmptyTable()
      

      if (last==true || countCounter == 2 ) {
        console.log('Done')
        return false
      }
    }


});

function giveEmptyTable(){
  let table = new sql.Table('Episode') // or temporary table, e.g. #temptable
    table.create = true
    table.columns.add('tconst', sql.VarChar(255), {nullable: false})
    table.columns.add("parentTconst ", sql.VarChar(255), { nullable: false });
    table.columns.add("seasonNumber ", sql.Int, {  nullable: true });
    table.columns.add("episodeNumber ", sql.Int, { nullable: true });

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