const lineReader = require("line-reader");
const env = require("dotenv").config();
const sql = require("mssql");

let bulkList = [];
let counter = 0;

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

let table = giveEmptyTable()[0];
let types = giveEmptyTable()[1];
let attributes = giveEmptyTable()[2]

lineReader.eachLine("akasdata.tsv", function (line, last) {
  if ((countCounter == 0) & (counter == 0)) {
    console.log("First line is " + line);
    line = "tt00000000	0	test	\N	\N	\N	0";
    console.log("First line is " + line);
  }

  counter++;
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
   stringBitToStringBoolean(array[7]),
  );


  if (array[5] != null) {
    let typesarray = array[5].split(",");

    for (let index = 0; index < typesarray.length; index++) {
      types.rows.add(array[0], typesarray[index]);
    }
  }

  if (array[6] != null) {
    let attributesarray = array[6].split(",");

    for (let index = 0; index < attributesarray.length; index++) {
      attributes.rows.add(array[0], attributesarray[index]);
    }
  }

  if (counter == 250000 || last == true) {
    insertData(table,types,attributes);
    counter = 0;
    countCounter++;
    table = giveEmptyTable()[0];
    types = giveEmptyTable()[1];
    attributes = giveEmptyTable()[2]
    if (last == true || countCounter == 35) {
      console.log("Done");
      return false;
    }
  }
});

function giveEmptyTable() {
  let table = new sql.Table("AKAS");
  table.create = true;
  table.columns.add("tconst", sql.VarChar(255), {nullable: false});
  table.columns.add("ordering", sql.Int, { nullable: true });
  table.columns.add("title", sql.NVarChar(sql.MAX), { nullable: true });
  table.columns.add("region", sql.VarChar(255), { nullable: true });
  table.columns.add("language", sql.VarChar(255), { nullable: true });
  table.columns.add("isOriginalTitle ", sql.Bit, { nullable: true });

    
  let type = new sql.Table("Types");
  type.create = true;
  type.columns.add("tconst", sql.VarChar(255), { nullable: false });
  type.columns.add("type", sql.VarChar(255), { nullable: false });

    
  let attributes = new sql.Table("Attributes");
  attributes.create = true;
  attributes.columns.add("tconst", sql.VarChar(255), { nullable: false });
  attributes.columns.add("attribute", sql.VarChar(255), { nullable: false });


  return [table,type,attributes];
}

function stringBitToStringBoolean(string){
  if (string == "0") {
    return "false"
  }else{
    return "true"
  }
}

async function insertData(table, types,attribute) {
  try {
    const poolPromise = new sql.ConnectionPool(sqlConfig)
      .connect()
      .then((pool) => {
        console.log("Connected to MSSQL");
        pool.request().bulk(table);
        pool.request().bulk(types);
        pool.request().bulk(attribute);
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
