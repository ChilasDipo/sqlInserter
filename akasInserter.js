const sql = require("mssql");
const fs = require("fs");
const readline = require("readline");

const config = {
  server: "localhost",
  port: 1433,
  user: "sa",
  password: 'YDLd"7exx8D}:~8G',
  options: {
    database: "IMDB4",
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

const fileStream = fs.createReadStream("data.tsv");

const rl = readline.createInterface({
  input: fileStream,
  crlfDelay: Infinity,
});

let counter = 0;

const table = new sql.Table("Movies");
table.create = true;
table.columns.add("tconst", sql.VarChar(255), {
  nullable: false,
  primary: true,
});
table.columns.add("titleId", sql.VarChar(255), { nullable: true });
table.columns.add("ordering", sql.VarChar(255), { nullable: true });
table.columns.add("title", sql.VarChar(255), { nullable: true });
table.columns.add("region", sql.VarChar(255), { nullable: true });
table.columns.add("language", sql.VarChar(255), { nullable: true });
table.columns.add("isOriginalTitle ", sql.Bit, { nullable: true });
