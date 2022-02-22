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
table.columns.add("titleType", sql.VarChar(255), { nullable: true });
table.columns.add("primaryTitle", sql.VarChar(255), { nullable: true });
table.columns.add("OriginalTitle", sql.VarChar(255), { nullable: true });
table.columns.add("isAdult", sql.VarChar(255), { nullable: true });
table.columns.add("startYear", sql.VarChar(255), { nullable: true });
table.columns.add("endYear", sql.VarChar(255), { nullable: true });
table.columns.add("runtimeMinutes", sql.VarChar(255), { nullable: true });

const tableTitleType = new sql.Table("TitleType");
tableTitleType.create = true;
tableTitleType.columns.add("ID", sql.VarChar(255), {
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

async function bulkInserts() {
  for await (const line of rl) {
    counter++;
    if (counter >= 50000) {
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
    request.bulk(tableGenres);
    request.bulk(tableGenresToTconst);
    request.bulk(tableTitleType);
    return request.bulk(table);
  })

  .then((data) => {
    console.log(data);
  })
  .catch((err) => {
    console.log(err);
  });
