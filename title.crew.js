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

let director = giveEmptyTable()[0]
let writer = giveEmptyTable()[1]

lineReader.eachLine('crewData.tsv', function(line,last) {

  if ((countCounter == 0) & (counter == 0)) {
    console.log("First line is " + line);
    line = "tt0000000	nm0000000	\N";
    console.log("First line is " + line);
  }

  counter++
  let array = line.split("\t");
      for (let index = 0; index < array.length; index++) {
        if (array[index] == "\\N") {
          array[index] = null;
        }
      }

      if (array[1] != null) {
        let directorarray = array[1].split(",");

        for (let index = 0; index < directorarray.length; index++) {
            director.rows.add(
            array[0],
            (directorarray[index])
          );
        }
      }


      if (array[2] != null) {
        let writerarray = array[2].split(",");

        for (let index = 0; index < writerarray.length; index++) {
            writer.rows.add(
            array[0],
            (writerarray[index])
          );
        }
      }
      
    if (counter==250000 || last==true) {
      insertData(director,writer)
      counter=0
     countCounter++
      director = giveEmptyTable()[0]
      writer = giveEmptyTable()[1]

      if (last==true || countCounter == 2)  {
        console.log('Done')
        return false
      }
    }

});

function giveEmptyTable(){
  let director = new sql.Table("Directors");
  director.create = true;
  director.columns.add("tconst", sql.VarChar(255), { nullable: false });
  director.columns.add("nconst", sql.VarChar(255), { nullable: true });
  
  let writer = new sql.Table("Writers");
  writer.create = true;
  writer.columns.add("tconst", sql.VarChar(255), { nullable: false });
  writer.columns.add("nconst", sql.VarChar(255), { nullable: true });
  
    
    return [director,writer ]
}

async function insertData(director,writer){
  try {
  
const poolPromise = new sql.ConnectionPool(sqlConfig)
  .connect()
  .then(pool => {
   console.log('Connected to MSSQL')
    pool.request().bulk(director);
    pool.request().bulk(writer);
    return pool
  })
  .catch(err => console.log('Database Connection Failed! Bad Config: ', err))



  }catch (error) {
    console.log(error)
    console.log(table)
    return
  }

}
