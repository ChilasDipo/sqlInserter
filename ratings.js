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

const fileStream = fs.createReadStream("ratingData.tsv");

const rl = readline.createInterface({
  input: fileStream,
  crlfDelay: Infinity,
});

const ratings = new sql.Table("Ratings");
ratings.create = true;
ratings.columns.add("tconst", sql.VarChar(255), {
  nullable: false,
  primary: true,
});
ratings.columns.add("averageRating", sql.Real, { nullable: true });
ratings.columns.add("numVotes", sql.Int, { nullable: true });

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
       console.log("Table added " + array)

      ratings.rows.add(
        array[0],
        parseFloat(array[1]),
        parseInt(array[2]),
      );

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
    return request.bulk(ratings);
  })

  .then((data) => {
    console.log(data);
  })
  .catch((err) => {
    console.log(err);
  });
