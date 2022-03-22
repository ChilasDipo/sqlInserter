

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

lineReader.eachLine('principalsData.tsv', function(line,last) {

  if ((countCounter == 0) & (counter == 0)) {
    console.log("First line is " + line);
    line = "tt0000000	0	nm0000000	test	\N	\N";
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
      parseInt(array[1]),
      array[2],
      array[3],
      array[4],
      array[5],
    );
      
    if (counter==250000 || last==true) {
      insertData(table)
      counter=0
     countCounter++
      table = giveEmptyTable()
      

      if (last==true  ) {
        console.log('Done')
        return false
      }
    }


});

function giveEmptyTable(){
  let table = new sql.Table('Principals') // or temporary table, e.g. #temptable
    table.create = true
    table.columns.add('tconst', sql.VarChar(255), {nullable: false})
    table.columns.add("ordering", sql.Int, { nullable: false });
    table.columns.add("nconst", sql.VarChar(255), {  nullable: true });
    table.columns.add("category", sql.NVarChar(sql.MAX), { nullable: true });
    table.columns.add("job", sql.NVarChar(sql.MAX), { nullable: true });
    table.columns.add("characters", sql.NVarChar(sql.MAX), { nullable: true });
    
    return table
}

async function insertData(table){
  try {
  
const poolPromise = new sql.ConnectionPool(sqlConfig)
  .connect()
  .then(pool => {
    console.log('Connected to MSSQL')
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