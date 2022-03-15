const lineReader = require('line-reader');
const env = require("dotenv").config();
const sql = require('mssql')
const fs = require('fs')
let counter = 0;
let bulkList = []



let countCounter = 0;

fs.appendFile('filedatabase.json', ` { "Ratings": [  \n`, function (err) {
  if (err) throw err;
  console.log('Updated!');
}); 


lineReader.eachLine('ratingData.tsv', function(line,last) {

  if (countCounter == 0 & counter == 0) {
   console.log("First line is " + line)
   line = "tt0000000	0.0	0000"
   console.log("First line is " + line)
  }

  counter++
  let array = line.split("\t");
      for (let index = 0; index < array.length; index++) {
        if (array[index] == "\\N") {
          array[index] = null;
        }
      }
      counter++;
      fs.appendFile('filedatabase.json', ` { "txonst" : "${ array[0]}",  "rating" : 
      "${ parseFloat(array[1])}", "numberOfVotes" : 
       "${  parseInt(array[2])}" } \n`, function (err) {
        if (err) throw err;
        console.log('Updated!');
      }); 

      
      
    if (counter==500 || last==true) {
       
        countCounter++;
        counter = 0
      if (last==true || countCounter == 1) {
        fs.appendFile('filedatabase.json', ` ] }  \n`, function (err) {
            if (err) throw err;
            console.log('Updated!');
          }); 
        return false
      }

    }else {
        
        fs.appendFile('filedatabase.json', ` , \n`, function (err) {
            if (err) throw err;
            console.log('comma added!');
          }); 
  }


});





