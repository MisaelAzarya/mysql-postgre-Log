const mysql = require('mysql');
const pg = require('pg');
const MySQLEvents = require('@rodrigogs/mysql-events');
const ora = require('ora'); // cool spinner
const spinner = ora({
  text: '🛸 Waiting for database events... 🛸',
  color: 'blue',
  spinner: 'dots2'
});

function ins(pgClient, table, data){
  return new Promise((resolve, reject) => {
    var qry = "INSERT INTO "+table+" VALUES "+data;
    pgClient.query(qry, function(err, results) {
      if (err) {
        console.error(err);
        return reject(err);
      }
      resolve(results);
    })
  })
}

function updt(pgClient, table, data, param_key, data_key){
  return new Promise((resolve, reject) => {
    pgClient.query("UPDATE "+table+" SET "+data+" WHERE "+param_key+" = '"+data_key+"'", function(err, results) {
      if (err) {
        console.error(err);
        return reject(err);
      }
      resolve(results);
    })
  })
}

function dlt(pgClient, table, param_key, data_key){
  return new Promise((resolve, reject) => {
    pgClient.query("DELETE FROM "+table+" WHERE "+param_key+" = '"+data_key+"'", function(err, results) {
      if (err) {
        console.error(err);
        return reject(err);
      }
      resolve(results);
    })
  })
}


const program = async () => {
  const connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: ''
  });

  const instance = new MySQLEvents(connection, {
    startAtEnd: true // to record only the new binary logs, if set to false or you didn'y provide it all the events will be console.logged after you start the app
  });

  //syntaxnya username:password@server:port/database_name
  const pgConString = "postgres://postgres:1234@localhost:5432/supersample"

  var clientpg = new pg.Client(pgConString);
  clientpg.connect();

  await instance.start();

  instance.addTrigger({
    name: 'monitoring all statments',
    expression: 'test.*', // listen to TEST database !!!
    statement: MySQLEvents.STATEMENTS.ALL, // you can choose only insert for example MySQLEvents.STATEMENTS.INSERT, but here we are choosing everything
    onEvent: e => {
      console.log(e);
      var table = "table1";
      
      if(e.type=='INSERT'){
        var full_data = [];
        var new_id = [];
        for(var x=0;x<e.affectedRows.length;x++){
          var data = [];
          var value = Object.values(e.affectedRows[x].after);
          var keys = Object.keys(e.affectedRows[x].after);
          for(var i=0;i<value.length;i++){
            data.push("'" + value[i] + "'");
          }
          new_id.push(value[0]);
          full_data.push('(' + data.join(', ') + ')');
        }
        // console.log(full_data);
        var promise = ins(clientpg,table,full_data);
        promise.then(function(result){
          spinner.succeed('ID = '+new_id+' Inserted');
          spinner.start();
        });
      }
      else if(e.type=='UPDATE'){ 
        var data = [];
        var value = Object.values(e.affectedRows[0].after);
        var keys = Object.keys(e.affectedRows[0].after);
        var param_key = keys[0];
        var data_key = value[0];
        for(var i=1;i<value.length;i++){
          data.push(keys[i] + " = '" + value[i] + "'");
        }
        // console.log(data);
        var promise = updt(clientpg,table,data,param_key,data_key);
        promise.then(function(result){
          spinner.succeed('ID = '+data_key+' Updated');
          spinner.start();
        });
      }else{ // untuk delete
        var value = Object.values(e.affectedRows[0].before);
        var keys = Object.keys(e.affectedRows[0].before);
        var param_key = keys[0];
        var data_key = value[0];
        var promise = dlt(clientpg,table,param_key,data_key);
        promise.then(function(result){
          spinner.succeed('ID = '+data_key+' Deleted');
          spinner.start();
        });
      }
    }
  });

  instance.on(MySQLEvents.EVENTS.CONNECTION_ERROR, console.error);
  instance.on(MySQLEvents.EVENTS.ZONGJI_ERROR, console.error);
};

program()
  .then(spinner.start.bind(spinner))
  .catch(console.error);