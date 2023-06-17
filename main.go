package main

import (
	"flag"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"strings"
	"time"
)

var (
	ndb = flag.String("ndb", "user=test_user password=test_pass host=localhost port=5432 dbname=new_db sslmode=disable", "new db connection string")
	odb = flag.String("odb", "user=test_user password=test_pass host=localhost port=5432 dbname=old_db sslmode=disable", "new db connection string")
)

func main() {
	flag.Parse()
	newDB, err := sqlx.Connect("postgres", *ndb)
	if err != nil {
		panic(err)
	}
	oldDB, err := sqlx.Connect("postgres", *odb)
	if err != nil {
		panic(err)
	}

	// get all tables from newDB
	var newTableNames []string
	sql := "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
	err = newDB.Select(&newTableNames, sql)
	if err != nil {
		panic(err)
	}
	// get all tables from oldDB
	var oldTableNames []string
	sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
	err = oldDB.Select(&oldTableNames, sql)
	if err != nil {
		panic(err)
	}
	//compare tables
	var migrationString string
	var createT []string
	var dropT []string

	otm := make(map[string]bool)
	ntm := make(map[string]bool)
	var sameTables []string

	// reorder old tables to map
	for _, t := range oldTableNames {
		otm[t] = true
	}

	// reorder new tables to map and check if we don't have new table in old tables so mark it to create
	for _, t := range newTableNames {
		ntm[t] = true
		_, ok := otm[t]
		if ok {
			continue
		}
		createT = append(createT, t)
	}

	// mark old tables not represented in new tables for drop
	for _, t := range oldTableNames {
		_, ok := ntm[t]
		if ok {
			sameTables = append(sameTables, t)
			continue
		}
		dropT = append(dropT, t)
	}

	if len(createT) > 0 {
		for _, c := range createT {
			// get table columns
			var cols []TableColumn
			sql = fmt.Sprintf("SELECT column_name, column_default, is_nullable, data_type, character_maximum_length "+
				"FROM information_schema.columns "+
				"WHERE table_schema = 'public' AND table_name = '%s'", c)
			err = newDB.Select(&cols, sql)
			if err != nil {
				fmt.Println(err)
				continue
			}
			var cStrings []string
			for _, col := range cols {
				cString := col.Name + " " + col.DataType
				if col.DataType == Varchar && col.Max != nil {
					cString += fmt.Sprintf("(%d)", *col.Max)
				}
				if col.IsNullable == NullableNO {
					cString += " NOT NULL"
				}
				if col.Default != nil {
					cString += fmt.Sprintf(" DEFAULT %s", *col.Default)
				}
				cStrings = append(cStrings, cString)
			}
			// get primary key
			var indexKey string
			sql = fmt.Sprintf("SELECT pg_attribute.attname "+
				"FROM pg_index, pg_class, pg_attribute, pg_namespace "+
				"WHERE pg_class.oid = '%s'::regclass AND indrelid = pg_class.oid "+
				"AND nspname = 'public' AND pg_class.relnamespace = pg_namespace.oid "+
				"AND pg_attribute.attrelid = pg_class.oid "+
				"AND pg_attribute.attnum = any(pg_index.indkey) "+
				"AND indisprimary", c)
			err = newDB.Get(&indexKey, sql)
			if err != nil {
				fmt.Println(fmt.Sprintf("primary key get problem %s", err.Error()))
			} else {
				cStrings = append(cStrings, fmt.Sprintf("PRIMARY KEY(%s)", indexKey))
			}
			migrationString += fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(\n\t%s\n);\n", c, strings.Join(cStrings, ",\n\t"))
			// get indexes
			var indexStrings []string
			sql = fmt.Sprintf("select indexdef from pg_indexes where tablename = '%s'", c)
			err = newDB.Select(&indexStrings, sql)
			if err == nil {
				for _, ind := range indexStrings {
					if !strings.Contains(ind, "_pkey") {
						migrationString += fmt.Sprintf("%s;\n", ind)
					}
				}
			}
		}
	}

	// add drop tables
	if len(dropT) > 0 {
		for _, dt := range dropT {
			migrationString += fmt.Sprintf("DROP TABlE IF EXISTS %s;\n", dt)
		}
	}

	// compare deep inside same tables
	createCols := make(map[string][]TableColumn)
	dropCols := make(map[string][]TableColumn)

	for _, st := range sameTables {
		ncm := make(map[string]TableColumn)
		ocm := make(map[string]TableColumn)
		var alters []string
		// get table columns
		var ncols []TableColumn
		sql = fmt.Sprintf("SELECT column_name, column_default, is_nullable, data_type, character_maximum_length "+
			"FROM information_schema.columns "+
			"WHERE table_schema = 'public' AND table_name = '%s'", st)
		err = newDB.Select(&ncols, sql)
		if err != nil {
			fmt.Println(err)
			continue
		}
		var ocols []TableColumn
		err = oldDB.Select(&ocols, sql)
		if err != nil {
			fmt.Println(err)
			continue
		}

		for _, ocol := range ocols {
			ocm[ocol.Name] = ocol
		}

		for _, ncol := range ncols {
			ncm[ncol.Name] = ncol
			_, ok := ocm[ncol.Name]
			if ok {
				if ncol.DataType != ocm[ncol.Name].DataType ||
					ncol.IsNullable != ocm[ncol.Name].IsNullable ||
					(ncol.Default != nil && *ncol.Default != *ocm[ncol.Name].Default) || (ncol.Max != nil && *ncol.Max != *ocm[ncol.Name].Max) {
					alterString := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", st, ncol.Name, ncol.DataType)
					if ncol.DataType == Varchar {
						if ncol.Max != nil && *ncol.Max != *ocm[ncol.Name].Max {
							alterString += fmt.Sprintf("(%d)", *ncol.Max)
						}
					}
					if ncol.IsNullable == NullableNO {
						alterString += fmt.Sprintf(" NOT NULL")
					}

					if ncol.Default != nil {
						alterString += fmt.Sprintf(" DEFAULT %s", *ncol.Default)
					}

					alters = append(alters, alterString)
				}

				continue
			}
			createCols[st] = append(createCols[st], ncol)
		}

		for _, ocol := range ocols {
			_, ok := ncm[ocol.Name]
			if ok {
				continue
			}
			dropCols[st] = append(dropCols[st], ocol)
		}

		if len(alters) > 0 {
			migrationString += strings.Join(alters, ";\n")
		}

	}

	// create new columns
	for tn, cc := range createCols {
		for _, c := range cc {
			acs := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tn, c.Name, c.DataType)
			if c.DataType == Varchar && c.Max != nil {
				acs += fmt.Sprintf("(%d)", *c.Max)
			}
			if c.IsNullable == NullableNO {
				acs += " NOT NULL"
			}
			if c.Default != nil {
				acs += fmt.Sprintf(" DEFAULT %s", *c.Default)
			}
			migrationString += acs + ";\n"
		}
	}

	//drop not existing columns
	for tn, cc := range dropCols {
		for _, c := range cc {
			acs := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tn, c.Name)
			migrationString += acs + ";\n"
		}
	}

	// process indexes
	for _, st := range sameTables {
		var nIndex []IndexDef
		sql = fmt.Sprintf("select indexname, indexdef from pg_indexes where tablename = '%s'", st)
		err = newDB.Select(&nIndex, sql)
		if err != nil {
			if !strings.Contains(err.Error(), "no rows") {
				continue
			}
		}

		var oIndex []IndexDef
		err = oldDB.Select(&oIndex, sql)
		if err != nil {
			if !strings.Contains(err.Error(), "no rows") {
				continue
			}
		}

		nix := make(map[string]string)
		oix := make(map[string]string)

		var createIndex []string
		var dropIndex []string

		for _, ni := range nIndex {
			nix[ni.Name] = ni.Definition
		}

		for _, oi := range oIndex {
			oix[oi.Name] = oi.Definition
			_, ok := nix[oi.Name]
			if ok {
				if nix[oi.Name] != oi.Definition {
					dropIndex = append(dropIndex, oi.Name)
					createIndex = append(createIndex, nix[oi.Name])
				}
				continue
			}
			dropIndex = append(dropIndex, oi.Name)
		}
		for _, ni := range nIndex {
			_, ok := oix[ni.Name]
			if ok {
				continue
			}
			createIndex = append(createIndex, ni.Definition)
		}

		for _, di := range dropIndex {
			migrationString += fmt.Sprintf("DROP INDEX %s;\n", di)
		}

		for _, ci := range createIndex {
			if !strings.Contains(ci, "_pkey") {
				migrationString += fmt.Sprintf("%s;\n", ci)
			}

		}
	}

	// finalize result with comment of migration version
	if len(migrationString) > 0 {
		migrationString = fmt.Sprintf("--- Migration Version %d\n", time.Now().Unix()) + migrationString
	}

	fmt.Println(migrationString)
}

type TableColumn struct {
	Name       string  `db:"column_name"`
	Default    *string `db:"column_default"`
	IsNullable string  `db:"is_nullable"`
	DataType   string  `db:"data_type"`
	Max        *int    `db:"character_maximum_length"`
}

type IndexDef struct {
	Name       string `db:"indexname"`
	Definition string `db:"indexdef"`
}

const (
	NullableNO  = "NO"
	NullableYes = "YES"
	Varchar     = "character varying"
)
