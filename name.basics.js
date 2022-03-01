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

const fileStream = fs.createReadStream("nameData.tsv");

const rl = readline.createInterface({
  input: fileStream,
  crlfDelay: Infinity,
});

const names = new sql.Table("Names");
names.create = true;
names.columns.add("nconst", sql.VarChar(255), {
  nullable: false,
  primary: true,
});
names.columns.add("primaryName", sql.VarChar(255), { nullable: true });
names.columns.add("birthYear", sql.VarChar(255), { nullable: true });
names.columns.add("deathYear", sql.VarChar(255), { nullable: true });

const primaryProfession = new sql.Table("PrimaryProfession");
primaryProfession.create = true;
primaryProfession.columns.add("nconst", sql.VarChar(255), { nullable: false });
primaryProfession.columns.add("profession", sql.VarChar(255), { nullable: true });


const knowForTitles = new sql.Table("KnownForTitles");
knowForTitles.create = true;
knowForTitles.columns.add("nconst", sql.VarChar(255), { nullable: false });
knowForTitles.columns.add("tconst", sql.VarChar(255), { nullable: false });

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

      names.rows.add(
        array[0],
        array[1],
        array[2],
        array[3],
      );

      if (array[4] != null) {
        let profession = array[4].split(",");

        for (let index = 0; index < profession.length; index++) {
          primaryProfession.rows.add(
            array[0],
            (profession[index])
          );
        }
      }

      if (array[5] != null) {
        let titles = array[5].split(",");

        for (let index = 0; index < titles.length; index++) {
          knowForTitles.rows.add(
            array[0],
            (titles[index])
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
    request.bulk(knowForTitles);
    request.bulk(primaryProfession);
    return request.bulk(names);
  })

  .then((data) => {
    console.log(data);
  })
  .catch((err) => {
    console.log(err);
  });
