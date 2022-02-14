const mysql = require('mysql');

var conn = mysql.createConnection({
	host: process.env.MYSQL_URI,
	user: process.env.MYSQL_USER,
	password: process.env.MYSQL_PASSWORD 
});



console.log(`Variable Info: ${conn.host}\n${conn.user}\n`)

conn.connect( function(err) {

	if(err) throw err;
	console.log("Connected!")

});
