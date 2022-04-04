//Importer Libaries som bliver brugt, et til at læse TSV filen, et til vores database koder og et til at snakke med databasen
const lineReader = require('line-reader');
const env = require("dotenv").config();
const sql = require('mssql')
//Counter for bulk insert den tæller hvor mange som er blevet sat ind
let counter = 0;

//Sql object til at komunikere med databasen
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

//counter for antalet af gange, hvor der er sket et bulk insert
let countCounter = 0;


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


function stringBitToStringBoolean(string){
  if (string == "0") {
    return false
  }else{
    return true
  }
}

let table = giveEmptyTable()[0]
let tableGenresToTconst = giveEmptyTable()[1]
lineReader.eachLine('data.tsv', function(line,last) {

  if ((countCounter == 0) & (counter == 0)) {
    console.log("First line is " + line);
  line = "tt0000000	short	Test	Test	0	1894	\N	1	Short";
  console.log("First line is " + line);
 }


  counter++
  let array = line.split("\t");
      for (let index = 0; index < array.length; index++) {
        if (array[index] == "\\N") {
          array[index] = null;
        }
      }

    table.rows.add(
      array[0],
      parseInt(titleType.indexOf(array[1]) + 1),
      array[2],
      array[3],
      stringBitToStringBoolean(array[4]),
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
      
    if (counter==250000 || last==true) {
      insertData(table,tableGenresToTconst)
      counter=0
     countCounter++
      table = giveEmptyTable()[0]
      tableGenresToTconst = giveEmptyTable()[1]

      if (last==true) {
        console.log('Done')
        return false
      }
    }


});

function giveEmptyTable(){
  let table = new sql.Table('Movies') // or temporary table, e.g. #temptable
    table.create = true
    table.columns.add('tconst', sql.VarChar(255), {nullable: false, primary: true})
    table.columns.add("titleType", sql.Int, { nullable: false });
    table.columns.add("primaryTitle", sql.NVarChar(sql.MAX), {  nullable: true });
    table.columns.add("OriginalTitle", sql.NVarChar(sql.MAX), { nullable: true });
    table.columns.add("isAdult", sql.Bit, { nullable: true });
    table.columns.add("startYear", sql.VarChar(255), { nullable: true });
    table.columns.add("endYear", sql.VarChar(255), { nullable: true });
    table.columns.add("runtimeMinutes", sql.VarChar(255), { nullable: true });
    

    let tableGenresToTconst = new sql.Table("GenresToTconst");
    tableGenresToTconst.create = true;
    tableGenresToTconst.columns.add("tconst", sql.VarChar(255), {
    nullable: false,
    });
    tableGenresToTconst.columns.add("Genre", sql.VarChar(255), { nullable: false });


    return [table,tableGenresToTconst]
}

async function insertData(table,tableGenresToTconst){
  try {
  
const poolPromise = new sql.ConnectionPool(sqlConfig)
  .connect()
  .then(pool => {
   console.log('Connected to MSSQL')
    pool.request().bulk(table);
    pool.request().bulk(tableGenresToTconst);
    return pool
  })
  .catch(err => console.log('Database Connection Failed! Bad Config: ', err))



  }catch (error) {
    console.log(error)
    console.log(table)
    return
  }

}