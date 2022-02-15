
require('dotenv').config();
const mysql = require('mysql');
const path = require('path');
const fs = require('fs');
const { parse } = require('csv-parse');

function createDataTable(conn, projectId, datasetId, newTableId) {

  newDataSetId = datasetId

  var table = {
    tableReference: {
      projectId: projectId,
      datasetId: newDataSetId,
      tableId: newTableId
    },
    schema: {
      fields: [
        {name: 'transactionDate', type: 'DATE'},
        {name: "transactionAmount", type: "FLOAT"},
        {name: "transactionStatus", type: "STRING"}
      ]
    }
  };

  var dataset = {
    datasetReference: {
      datasetId: newDataSetId
    }
  };

//  BigQuery.Datasets.insert(dataset, projectId)
//  BigQuery.Tables.insert(table, projectId, datasetId)
}

function insertDataIntoMySql(conn, theBlob, projectId, tableId, theJob) {

 //  job = BigQuery.Jobs.insert(theJob, projectId, theBlob)
}

function importCSVintoMySql(conn, theBlob, rowIncludeFlag, bqProjectId, bqDatasetId, bqTableId) {

	  var job = {
	    configuration: {
	      load: {
		destinationTable: {
		  projectId: bqProjectId,
		  datasetId: bqDatasetId,
		  tableId: bqTableId
		},
		skipLeadingRows: rowIncludeFlag
	      }
	    }
	  };

	console.log(theBlob)

	conn.query('insert into coejr_learning.tbl_ubereatstransactions(region, orderID, OrderID2, transactionDate, transactionStatus, productNameShort, productNameLong, misc, itemCost, transactionAmount, transactionCurrency) VAlUES ?', [theBlob], function(err, rows) {
			if(err) {
				console.error('Failure occurred: ' + err.message)
			}
			else {
				console.log('Number of rows: ' + rows)
			}
	});

}

function getCSVContentBlob(theCSVIds, theProjectId, theDataSetId, theTableId, flg_includeHeader) {

  for(i = 0; i < theCSVIds.length; i++) {
      // Open the first CSV file, ingest headings and data
      // var firstFile = DriveApp.getFileById(theCSVIds[i])
      var firstFileData = firstFile.getBlob().setContentType('application/octet-stream')

      if((i == 0) && (flg_includeHeader)) {
        importCSVintoSnowflake(firstFileData, 0, theProjectId, theDataSetId, theTableId)
      }
      else {
        importCSVintoSnowflake(firstFileData, 1, theProjectId, theDataSetId, theTableId)
      }
      console.log(theCSVIds[i] + ": ")
      console.log(firstFileData)
  }

}

function dateFix(m_line) {

        t_month = m_line.substring(m_line.length-24,m_line.length-22)
        t_day = m_line.substring(m_line.length-21,m_line.length-19)
        t_year = m_line.substring(m_line.length-29,m_line.length-25)

        fixed_date = t_year + "-" + t_month + "-" + t_day

        return fixed_date

}

function amountFix(m_line) {

	if(m_line.length == 0) {
		return 0
	}

	console.log("Passed to amount fix: " + m_line)

	n_regex = new RegExp("\\$", "gi")

	m_line_fix = m_line.replace(n_regex, "")

	console.log("Result of amount fix: " + m_line_fix)

	return m_line_fix

}


function getCSVContentArray(conn, theCSVIds, theProjectId, theDataSetId, theTableId, flg_includeHeader, theFolderName) {

  myFolder = path.join(__dirname, theFolderName)
  let j = 0
  let theBlob = ""
  let t_record = []
  let t_record_sub = []
	var firstFileDataCSV = []
	var firstFileDataRow = []
	var dataPack = []
	var valueArray = []
	var headerArray = []
	insertedFlag = false
  for(i = 0; i < theCSVIds.length; i++) {

	fs.createReadStream(theCSVIds[i])
		.pipe(parse({delimiter: ','}))
		.on('data', function(firstFileDataCSV) {

			firstFileDataRow.push(firstFileDataCSV)	
	})
	.on('end', function() {
      		// t_record.push([firstFileDataRow])
		for(j = 0; j < firstFileDataRow.length; j++) {
			if(!isNaN(parseFloat(firstFileDataRow[j][8]))) {
				t_db_transactionDate = dateFix(firstFileDataRow[j][3])
				t_db_transactionRegion = amountFix(firstFileDataRow[j][0])
				t_db_transactionItemCost = amountFix(firstFileDataRow[j][8])
				t_db_transactionAmount = amountFix(firstFileDataRow[j][9])
				valueArray.push([t_db_transactionRegion, firstFileDataRow[j][1], firstFileDataRow[j][2], t_db_transactionDate, firstFileDataRow[j][4], firstFileDataRow[j][5], firstFileDataRow[j][6], firstFileDataRow[j][7], parseFloat(t_db_transactionItemCost), parseFloat(t_db_transactionAmount), firstFileDataRow[j][10]])
			} else {
				headerArray.push([firstFileDataRow[j][0], firstFileDataRow[j][1], firstFileDataRow[j][2]])
			}
//		}
			if(!insertedFlag) {
				// insertedFlag = true
			}
		}
		if(!insertedFlag) {
			console.log(headerArray)
			console.log(valueArray)
			// var myTemp = valueArray
			// console.log(myTemp)
			importCSVintoMySql(conn, valueArray, 0, theProjectId, theDataSetId, theTableId)
			// valueArray.pop()
			insertedFlag = true
			console.log(valueArray)
		}
	});

	}
}

function getCSVFiles(conn, theFolderName, theProjectId, theDataSetId, theTableId) {
  myFolder = path.join(__dirname, theFolderName)
  var csvIDs = []


   fs.readdir(myFolder, function(err, myFiles) {

	if(err) {
		return console.log('Unable to scan directory: ' + err)
	}
	myFiles.forEach(function(file) {
		csvIDs.push(myFolder + "/" + file)
		// console.log(csvIDs);
		// getCSVContentsBlob(csvIDs, theProjectId, theDataSetId, theTableId, false)
   		// console.log(csvIDs.length + " : " )

   	});
	getCSVContentArray(conn, csvIDs, theProjectId, theDataSetId, theTableId, false, theFolderName)
	console.log(csvIDs)
    });
}

function main(conn) {
  projectId = "midyear-glazing-196002"
  folderName = "rcjUberEatsTransactions"
  datasetId = 'ds_' + new Date().getTime()
  tableId = 'tbl_gasbill'

  // createDataTable(conn, projectId, datasetId, tableId)
  getCSVFiles(conn, folderName, projectId, datasetId, tableId)
}


function loadTest(conn) {
  projectId = "midyear-glazing-196002"
  folderName = "rcjUberEatsTransactions"
  datasetId = "ds_1644416200996"
  tableId = "table_1644416200996"

  // createDataTable(projectId, datasetId, tableId)
  getCSVFiles(conn, folderName, projectId, datasetId, tableId)
}

function connectUp() {


	var connection = mysql.createConnection( {
		user: process.env.MYSQL_USER,
		host: process.env.MYSQL_URI,
		password: process.env.MYSQL_PASSWORD
	});

	connection.user = process.env.MYSQL_USER
	connection.host = process.env.MYSQL_URI
	connection.password = process.env.MYSQL_PASSWORD

	connection.connect(
		function(err, conn) {

			if(err) {
				console.error('Unable to connect: ' + err.message);
			}
			else {
				console.log('Successfully connected to MySql.');
			// 	connection_ID = conn.getId();
			}

		}
	);
	console.log(connection.user)
	console.log(connection.host)
/*
	connection.execute({
		// sqlText: 'use warehouse COMPUTE_WH_XL; insert into COEJR_LEARNING.COE_SANDBOX.COE_GASBILL(transactionDate, transactionStatus) VAlUES(?, ?)',
		sqlText: 'ALTER SESSION SET LOCK_TIMEOUT = 3600',
		// binds: theBlob,
		// binds: [['2022-02-10', 550.20, 'Received']],
		complete: function(err, stmt, rows) {
			if(err) {
				console.error('Failure occurred: ' + err.message)	
			}
			else {
				console.log('Number of rows: ' + rows.length)
			}
		}
	}); */

	main(connection)

}



connectUp()


