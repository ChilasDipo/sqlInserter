const sql = require("mssql");
const fs = require("fs");
const readline = require("readline");
const env = require("dotenv").config();

const config = {
  server: process.env.server,
  port: parseInt(process.env.port),
  user: process.env.user,
  password: process.env.password,
  options: {
    database: process.env.database,
  },
  option: {
    enableArithAbort: true,
    trustServerCertificate: true,
  },
  trustServerCertificate: true,
  connectionTimeout: 150000,
  pool: {
    max: 10,
    min: 0,
  },
};

const fileStream = fs.createReadStream("crewData.tsv");

const rl = readline.createInterface({
  input: fileStream,
  crlfDelay: Infinity,
});

const director = new sql.Table("Directors");
director.create = true;
director.columns.add("tconst", sql.VarChar(255), { nullable: false });
director.columns.add("nconst", sql.VarChar(255), { nullable: true });

const writer = new sql.Table("Writers");
writer.create = true;
writer.columns.add("tconst", sql.VarChar(255), { nullable: false });
writer.columns.add("nconst", sql.VarChar(255), { nullable: true });

let counter = -1 

async function bulkInserts() {
  for await (const line of rl) {
    counter++;
    if(counter == 0){
      continue
    }
    if (counter >= 10) {
      console.log("counter is 50000");
      let sluttid = new Date();
      console.log("Sluttid er" + sluttid);
      break;
    }

    let array = line.split("\t");
    for (let index = 0; index < array.length; index++) {
      if (array[index] == "\\N") {
        array[index] = null;
      }
    }

    try {
      // console.log("Table added " + array)

        
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


    } catch (err) {
      // ... error checks
      console.log("Error");
      console.log(err.message);
    }
  }
}
bulkInserts().then();
sql
  .connect(config)
  .then(() => {
    console.log("connected");

    const request = new sql.Request();
    request.bulk(director);
    return request.bulk(writer);
  })

  .then((data) => {
    console.log(data);
  })
  .catch((err) => {
    console.log(err);
  });
