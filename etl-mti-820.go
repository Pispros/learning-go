package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/microsoft/go-mssqldb"
)

var db *sql.DB

var server = "mit820dbserver.database.windows.net"

var port = 1433

var user = "CloudSAb454d64f"

var password = "MTI820PasswordBixi"

var database = "mti820Database"

func main() {

	if len(os.Args) >= 4 {

		var tableToInsert string = os.Args[1]

		var databaseToInsert string = os.Args[2]

		var filePath string = os.Args[3]

		// Build connection string

		connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",

			server, user, password, port, databaseToInsert)

		var err error

		// Create connection pool

		db, err = sql.Open("sqlserver", connString)

		if err != nil {

			log.Fatal("Error creating connection pool: ", err.Error())

		}

		ctx := context.Background()

		err = db.PingContext(ctx)

		if err != nil {

			log.Fatal(err.Error())

		}

		fmt.Printf("Connected!")

		fmt.Println(" ----------- Starting extraction and insertion")

		file, err := os.Open(filePath)

		if err != nil {
			log.Fatal("Error while reading the file", err)
		}

		defer file.Close()

		reader := csv.NewReader(file)

		records, err := reader.ReadAll()

		if err != nil {
			fmt.Println("Error reading records")
		}

		//Connexion to Local database
		// var connectionString string = "go:Passer1234@tcp(127.0.0.1:3306)/" + databaseToInsert

		// db, err := sql.Open("mysql", connectionString)

		// if err != nil {
		// 	panic(error.Error(err))
		// }

		// defer db.Close()

		var tableStructString string = "("

		for i := 0; i < len(records[0]); i++ {
			tableStructString += records[0][i] + ","
		}
		tableStructString = tableStructString[0:len(tableStructString)-1] + ")"
		fmt.Println(tableStructString)

		records = records[1:len(records)]

		for _, eachrecord := range records {
			var dataToInsert string = ""
			for i := 0; i < len(eachrecord); i++ {
				eachrecord[i] = strings.ReplaceAll(eachrecord[i], "'", "")
				dataToInsert += "'" + eachrecord[i] + "',"
			}
			dataToInsert = dataToInsert[0 : len(dataToInsert)-1]
			var insertString string = "INSERT INTO " + tableToInsert + tableStructString + " VALUES(" + dataToInsert + ");"
			fmt.Println(insertString)
			insert, err := db.Query(insertString)

			if err != nil {
				panic(error.Error(err))
			}

			insert.Close()
		}
		fmt.Println(" -----------All Data inserted successfully")
	} else {
		fmt.Println("")
		fmt.Println("Veuillez fournir une base de données et une source dans les paramètres")
		fmt.Println("")
		fmt.Println("----------- > go run ex.go parametre1 parametre2 parametre3")
		fmt.Println("")
		fmt.Println("parametre 1 = table de destination")
		fmt.Println("parametre 2 = Base de donnée")
		fmt.Println("parametre 3 = fichier csv source")
		fmt.Println("-")
	}
}
