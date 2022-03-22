const lineReader = require("line-reader");
const env = require("dotenv").config();
const sql = require("mssql");

let counter = 0;
let bulkList = [];

const sqlConfig = {
  user: process.env.user,
  password: process.env.password,
  database: process.env.database,
  server: "localhost",
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
  options: {
    encrypt: true, // for azure
    trustServerCertificate: true, // change to true for local dev / self-signed certs
  },
};

let countCounter = 0;

let names = giveEmptyTable()[0];
let primaryProfession = giveEmptyTable()[1];
let knowForTitles = giveEmptyTable()[2];

lineReader.eachLine("nameData.tsv", function (line, last) {
  if ((countCounter == 0) & (counter == 0)) {
     console.log("First line is " + line);
   line = "nm0000000	test	0000	0000	test,test,test	test,test,test,test";
   console.log("First line is " + line);
  }

  counter++;
  let array = line.split("\t");
  for (let index = 0; index < array.length; index++) {
    if (array[index] == "\\N") {
      array[index] = null;
    }
  }

  names.rows.add(array[0], array[1], array[2], array[3]);

  if (array[4] != null) {
    let profession = array[4].split(",");

    for (let index = 0; index < profession.length; index++) {
      primaryProfession.rows.add(array[0], profession[index]);
    }
  }

  if (array[5] != null) {
    let titles = array[5].split(",");

    for (let index = 0; index < titles.length; index++) {
      knowForTitles.rows.add(array[0], titles[index]);
    }
  }

  if (counter == 250000 || last == true) {
    insertData(names, primaryProfession, knowForTitles);
    counter = 0;
    countCounter++;
    names = giveEmptyTable()[0];
    primaryProfession = giveEmptyTable()[1];
    knowForTitles = giveEmptyTable()[2];

    if (last == true) {
      return false;
    }
  }
});

function giveEmptyTable() {
  let names = new sql.Table("Names");
  names.create = true;
  names.columns.add("nconst", sql.VarChar(255), {
    nullable: false,
    primary: true,
  });
  names.columns.add("primaryName", sql.VarChar(255), { nullable: true });
  names.columns.add("birthYear", sql.VarChar(255), { nullable: true });
  names.columns.add("deathYear", sql.VarChar(255), { nullable: true });

  let primaryProfession = new sql.Table("PrimaryProfession");
  primaryProfession.create = true;
  primaryProfession.columns.add("nconst", sql.VarChar(255), {
    nullable: false,
  });
  primaryProfession.columns.add("profession", sql.VarChar(255), {
    nullable: true,
  });

  let knowForTitles = new sql.Table("KnownForTitles");
  knowForTitles.create = true;
  knowForTitles.columns.add("nconst", sql.VarChar(255), { nullable: false });
  knowForTitles.columns.add("tconst", sql.VarChar(255), { nullable: false });

  return [names, primaryProfession, knowForTitles];
}

async function insertData(names, primaryProfession, knowForTitles) {
  try {
    const poolPromise = new sql.ConnectionPool(sqlConfig)
      .connect()
      .then((pool) => {
        // console.log('Connected to MSSQL')
        pool.request().bulk(names);
        pool.request().bulk(primaryProfession);
        pool.request().bulk(knowForTitles);
        return pool;
      })
      .catch((err) =>
        console.log("Database Connection Failed! Bad Config: ", err)
      );
  } catch (error) {
    console.log(error);
    console.log(table);
    return;
  }
}
