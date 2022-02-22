const fs = require("fs");
const readline = require("readline");
const sql = require("mssql");

const config = {
  server: "localhost",
  port: 1433,
  user: "sa",
  password: 'YDLd"7exx8D}:~8G',
  database: "IMDB",
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

sql.on("error", (err) => {
  console.log(err.message);
});

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

let startid = new Date()
console.log('start tid er ' + startid)
function stringToBoolean(value) {
  if (value == 0) {
    return false;
  } else {
    return True;
  }
}
let counter = 0;
async function processLineByLine() {
  const fileStream = fs.createReadStream("data.tsv");

  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });
  // Note: we use the crlfDelay option to recognize all instances of CR LF

  for await (const line of rl) {
    counter++
    if (counter >= 50000) {
        console.log('counter is 50000')
        break
    }

    // Each line in input.txt will be successively available here as `line`.
    let array = line.split("\t");
    for (let index = 0; index < array.length; index++) {
      if (array[index] == "\\N") {
        array[index] = null;
      }
    }

    try {
      let pool = await sql.connect(config);
      let genreArray = array[8].split(",");

     // console.log("Arrayet er " + array);

      let request = pool.request();
      request.input("tconst", sql.VarChar, array[0]);
      request.input("titleType", sql.VarChar, array[1]);
      request.input("primaryTitle", sql.VarChar, array[2]);
      request.input("OriginalTitle", sql.VarChar, array[3]);
      request.input("isAdult", sql.Bit, stringToBoolean(array[4]));
      request.input("startYear", sql.Int, array[5]);
      request.input("endYear", sql.Int, array[6]);
      request.input("runtimeMinutes", sql.Int, array[7]);

      let result = await request.query(
        `INSERT INTO IMDB_Movies([tconst],[titleType],[primaryTitle],[originalTitle],[isAdult],[startYear],[endYear],[runtimeMinutes])VALUES(@tconst,(Select ID From TitleType Where titleType=@titleType),@primaryTitle ,@OriginalTitle,@isAdult ,@startYear ,@endYear,@runtimeMinutes)`
      );
      //console.log(result)

      for (let index = 0; index < genreArray.length; index++) {
        // console.log(genreArray[index]);
        let requestGenre = await pool
          .request()
          .query(
            `INSERT INTO GenresTOTitleID([tconst],[genres]) Values ( '${
              array[0]
            }' , ${genreList.indexOf(genreArray[index]) + 1})`
          );
        // console.log(requestGenre);
      }
    
      sql.close();
    } catch (err) {
      // ... error checks
      console.log("Error");
      console.log(err.message);
      sql.close();
    }
  }
  let sluttid = new Date()
  console.log('Sluttid er' + sluttid)

}

processLineByLine();
