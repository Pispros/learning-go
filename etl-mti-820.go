package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func main() {

	if len(os.Args) >= 5 {

		var tableType string = os.Args[1]

		var tableToInsert string = os.Args[2]

		var databaseToInsert string = os.Args[3]

		var filePath string = os.Args[4]

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

		var connectionString string = "go:Passer1234@tcp(127.0.0.1:3306)/" + databaseToInsert

		db, err := sql.Open("mysql", connectionString)

		if err != nil {
			panic(error.Error(err))
		}

		defer db.Close()

		// Shift first line
		records = records[1:len(records)]

		if tableType == "station" {
			for _, eachrecord := range records {
				var insertString string = "INSERT INTO " + tableToInsert + " VALUE(\"" + eachrecord[0] + "\",\"" + eachrecord[1] + "\",\"" + eachrecord[2] + "\",\"" + eachrecord[3] + "\");"
				fmt.Println(insertString)
				insert, err := db.Query(insertString)

				if err != nil {
					panic(error.Error(err))
				}

				insert.Close()
			}
		}

		if tableType == "deplacement" {
			for _, eachrecord := range records {
				var insertString string = "INSERT INTO " + tableToInsert + " VALUE(\"" + eachrecord[0] + "\",\"" + eachrecord[1] + "\",\"" + eachrecord[2] + "\",\"" + eachrecord[3] + "\",\"" + eachrecord[4] + "\");"
				fmt.Println(insertString)
				insert, err := db.Query(insertString)

				if err != nil {
					panic(error.Error(err))
				}

				insert.Close()
			}
		}

		fmt.Println(" -----------All Data inserted successfully")
	} else {
		fmt.Println("")
		fmt.Println("Veuillez fournir une base de données et une source dans les paramètre")
		fmt.Println("")
		fmt.Println("----------- > go run ex.go parametre1 parametre2 parametre3 parametre4")
		fmt.Println("")
		fmt.Println("parametre 1 = type de table")
		fmt.Println("parametre 2 = table de destination")
		fmt.Println("parametre 3 = Base de donnée")
		fmt.Println("parametre 4 = fichier csv source")
		fmt.Println("")
	}
}
