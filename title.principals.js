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

//Opsætter table hvor der kan sætte data ind i (See giveEmptyTable function for table stukture)
let table = giveEmptyTable()

//Read the TSV file LINE by LINE and Call the function for each line
lineReader.eachLine('principalsData.tsv', function(line,last) {

  //Checks if you are reading the first line of the TSV file and changes the line to be this temp line, witch will be removed in the database later (This could be done better with a akip line but that dident want to work)
  if ((countCounter == 0) & (counter == 0)) {
    console.log("First line is " + line);
    line = "tt0000000	0	nm0000000	test	\N	\N";
    console.log("First line is " + line);
  }
// Counts each line 
  counter++
//SPlit the line at each TAB and puts the values into an array  and changes the \N from the TSV files to NULL
  let array = line.split("\t");
      for (let index = 0; index < array.length; index++) {
        if (array[index] == "\\N") {
          array[index] = null;
        }
      }

      //Puts the  values from the array into the table in the column where it fits
    table.rows.add(
      array[0],
      parseInt(array[1]),
      array[2],
      array[3],
      array[4],
      array[5],
    );
      
    //Checks if the counter is at 250000 or it is the last line 
    if (counter==250000 || last==true) {
      //Gives the table to the insert function witch sends it to the database
      insertData(table)
      //resets counter
      counter=0
     countCounter++
      table = giveEmptyTable()
      
      //Checks if is the last line and ends the real line if it is the last one 
      if (last==true  ) {
        console.log('Done')
        return false
      }
    }


});

//Function som opbygger sql tablen
function giveEmptyTable(){
  let table = new sql.Table('Principals') // or temporary table, e.g. #temptable
    table.create = true
    table.columns.add('tconst', sql.VarChar(255), {nullable: false})
    table.columns.add("ordering", sql.Int, { nullable: false });
    table.columns.add("nconst", sql.VarChar(255), {  nullable: true });
    table.columns.add("category", sql.NVarChar(sql.MAX), { nullable: true });
    table.columns.add("job", sql.NVarChar(sql.MAX), { nullable: true });
    table.columns.add("characters", sql.NVarChar(sql.MAX), { nullable: true });
    return table
}

async function insertData(table){
  try {  
    //Makes the connection obejct for taking to the database and uses it to add the table to the database
const poolPromise = new sql.ConnectionPool(sqlConfig)
  .connect()
  .then(pool => {
    console.log('Connected to MSSQL')
    pool.request().bulk(table);
    return pool
  })
  .catch(err => console.log('Database Connection Failed! Bad Config: ', err))
  }catch (error) {
    console.log(error)
    console.log(table)
    return
  }

}