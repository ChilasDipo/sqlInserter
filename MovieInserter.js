"use strict";

let startid = new Date();
console.log("start tid er " + startid);



const genreList = [
  "Action",
  "Adventure",
  "Animation",
  "Biography",
  "Comedy",
  "Crime",
  "Documentary",
  "Drama",
  "Family",
  "Fantasy",
  "Film Noir",
  "Game-Show",
  "History",
  "Horror",
  "Music",
  "Musical",
  "Mystery",
  "News",
  "Reality-TV",
  "Romance",
  "Sci-Fi",
  "Short",
  "Sport",
  "Talk-Show",
  "Thriller",
  "War",
  "Western",
];

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

const sql = require('mssql/msnodesqlv8')
const fs = require("fs");
const readline = require("readline");
const env = require("dotenv").config();

const config = {
  driver: "msnodesqlv8",
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
  connectionTimeout: 1500000,
  pool: {
    max: 100,
    min: 0,
    idleTimeoutMillis: 30000,
  },
};

const fileStream = fs.createReadStream("data.tsv");

const rl = readline.createInterface({
  input: fileStream,
  crlfDelay: Infinity,
});
let counterForStopping = -1;
let counterForDataSending = -1;

const tableTitleType = new sql.Table("TitleType");
tableTitleType.create = true;
tableTitleType.columns.add("ID", sql.Int, {
  nullable: false,
  primary: true,
});
tableTitleType.columns.add("TitleType", sql.VarChar(255), { nullable: false });

for (let index = 0; index < titleType.length; index++) {
  tableTitleType.rows.add((index + 1).toString(), titleType[index].toString());
}

const tableGenres = new sql.Table("Genres");
tableGenres.create = true;
tableGenres.columns.add("ID", sql.VarChar(255), {
  nullable: false,
  primary: true,
});
tableGenres.columns.add("Genre", sql.VarChar(255), { nullable: false });
console.log(genreList);
for (let index = 0; index < genreList.length; index++) {
  tableGenres.rows.add((index + 1).toString(), genreList[index].toString());
}

const tableGenresToTconst = new sql.Table("GenresToTconst");
tableGenresToTconst.create = true;
tableGenresToTconst.columns.add("tconst", sql.VarChar(255), {
  nullable: false,
});
tableGenresToTconst.columns.add("Genre", sql.VarChar(255), { nullable: false });

//let request = new sql.Request();
//request.bulk(tableGenres);
//request.bulk(tableGenresToTconst);
//request.bulk(tableTitleType);







async function messageHandler() {
  await conn; // ensures that the pool has been created
  try {
      const request = pool.request(); // or: new sql.Request(pool1)
      const result =  request.bulk(table, (err, rowCount) => {
        console.log(err);
        console.log(rowCount);
    })
      //console.dir(result)
      return result;
  } catch (err) {
      console.error('SQL error', err);
  }
}


async function bulkInserts() {
  let table = new sql.Table("Movies");
table.create = true;
table.columns.add("tconst", sql.VarChar(255), {
  nullable: false,
  primary: true,
});
table.columns.add("titleType", sql.Int, { nullable: false });
table.columns.add("primaryTitle", sql.VarChar(255), { nullable: true });
table.columns.add("OriginalTitle", sql.VarChar(255), { nullable: true });
table.columns.add("isAdult", sql.VarChar(255), { nullable: true });
table.columns.add("startYear", sql.VarChar(255), { nullable: true });
table.columns.add("endYear", sql.VarChar(255), { nullable: true });
table.columns.add("runtimeMinutes", sql.VarChar(255), { nullable: true });
  for await (const line of rl) {
    try {
      counterForStopping++;
      if (counterForStopping == 500000) {
        console.log("Stopped" + counterForStopping);
       
        adddata(table)
      
        table = new sql.Table("Movies");
        table.create = true;
        table.columns.add("tconst", sql.VarChar(255), {
          nullable: false,
          primary: true,
        });
        table.columns.add("titleType", sql.Int, { nullable: false });
        table.columns.add("primaryTitle", sql.VarChar(255), { nullable: true });
        table.columns.add("OriginalTitle", sql.VarChar(255), { nullable: true });
        table.columns.add("isAdult", sql.VarChar(255), { nullable: true });
        table.columns.add("startYear", sql.VarChar(255), { nullable: true });
        table.columns.add("endYear", sql.VarChar(255), { nullable: true });
        table.columns.add("runtimeMinutes", sql.VarChar(255), { nullable: true });
        

        counterForStopping = 0;
      
      }


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

      if (array[8] != null) {
        let genreArray = array[8].split(",");

        for (let index = 0; index < genreArray.length; index++) {
          tableGenresToTconst.rows.add(
            array[0],
            (genreList.indexOf(genreArray[index]) + 1).toString()
          );
        }
      }
    } catch (err) {
      console.log(err);
    }
  }
}

bulkInserts()
 
async function executeQuery(table) {
  
  return sql.connect(config).then(pool => {
      // Query    
   
      return pool.request().bulk(table)
  }).then(result => {
      
  }).catch(err => {
     console.log(err)
  })
}
async function getData(table) {
  await executeQuery(table);
 
}

async function adddata(table){
  let cnn = await sql.connect(config)

// query
let result = await sql.bulk(table)

// close connection
await cnn.close()
}
