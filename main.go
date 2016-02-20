package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

var cIP = flag.String("h", "127.0.0.1", "ip of a running cluster node")

//var cPORT = flag.String("p", "9042", "port number for the cluster node")
var cUSER = flag.String("user", "", "Username for the cluster")
var cPASS = flag.String("pass", "", "Password for the cluster")

var con *gocql.Session

func main() {
	flag.Parse()
	var err error
	con, err = connect()
	if err != nil {
		log.Fatal(err)
	}
	KEYSPACES := []string{}
	iter := con.Query("select keyspace_name from system_schema.keyspaces  ;").Iter()
	var keyspace string
	for iter.Scan(&keyspace) {
		if keyspace == "system_schema" ||
			keyspace == "system_auth" ||
			keyspace == "system" ||
			keyspace == "system_traces" ||
			keyspace == "system_distributed" {
			continue
		}
		KEYSPACES = append(KEYSPACES, keyspace)
	}
	FatalIfError(iter.Close())
	log.Print(KEYSPACES)
	for _, keyspace := range KEYSPACES {
		dumpKeyspace(keyspace)
	}
}

func dumpKeyspace(keyspace string) {
	log.Println("-- dumping keyspace", keyspace)
	dumpKeyspaceTypes(keyspace)
	tables := dumpKeyspaceTablesDef(keyspace)
	dumpData(keyspace, tables)
}

type Table struct {
	Keyspace_name               string
	Table_name                  string
	Bloom_filter_fp_chance      float64
	Caching                     map[string]string
	Comment                     string
	Compaction                  map[string]string
	Compression                 map[string]string
	Crc_check_chance            float64
	Dclocal_read_repair_chance  float64
	Default_time_to_live        int32
	Extensions                  map[string][]byte
	Flags                       []string
	Gc_grace_seconds            int32
	Id                          gocql.UUID
	Max_index_interval          int32
	Memtable_flush_period_in_ms int32
	Min_index_interval          int32
	Read_repair_chance          float64
	Speculative_retry           string
	Columns                     []Column
}

type Column struct {
	Keyspace_name     string
	Table_name        string
	Column_name       string
	Clustering_order  string
	Column_name_bytes []byte
	Kind              string
	Position          int32
	Type              string
}
type ColumnByPosition []Column

func (a ColumnByPosition) Len() int           { return len(a) }
func (a ColumnByPosition) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ColumnByPosition) Less(i, j int) bool { return a[i].Position < a[j].Position }

func dumpKeyspaceTablesDef(keyspace string) []*Table {
	log.Println("-- dumping keyspace", keyspace, "tables definition")
	var tables []*Table
	iter := con.Query(`SELECT 
				keyspace_name,
				table_name,
				bloom_filter_fp_chance,
				caching,
				comment,
				compaction,
				compression,
				crc_check_chance,
				dclocal_read_repair_chance,
				default_time_to_live,
				extensions,
				flags,
				gc_grace_seconds,
				id,
				max_index_interval,
				memtable_flush_period_in_ms,
				min_index_interval,
				read_repair_chance,
				speculative_retry 
			FROM system_schema.tables 
			WHERE keyspace_name = ?`, keyspace).Iter()
	var t *Table
	t = &Table{}
	for iter.Scan(
		&t.Keyspace_name,
		&t.Table_name,
		&t.Bloom_filter_fp_chance,
		&t.Caching,
		&t.Comment,
		&t.Compaction,
		&t.Compression,
		&t.Crc_check_chance,
		&t.Dclocal_read_repair_chance,
		&t.Default_time_to_live,
		&t.Extensions,
		&t.Flags,
		&t.Gc_grace_seconds,
		&t.Id,
		&t.Max_index_interval,
		&t.Memtable_flush_period_in_ms,
		&t.Min_index_interval,
		&t.Read_repair_chance,
		&t.Speculative_retry) {
		// ---
		tables = append(tables, t)
		t = &Table{}
	}
	FatalIfError(iter.Close())
	for _, table := range tables {
		iter := con.Query(`SELECT
					keyspace_name,
					table_name,
					column_name,
					clustering_order,
					column_name_bytes,
					kind,
					position,
					type
				FROM system_schema.columns 
				WHERE keyspace_name = ? AND table_name = ?`, keyspace, table.Table_name).Iter()
		var c Column
		for iter.Scan(&c.Keyspace_name, &c.Table_name, &c.Column_name, &c.Clustering_order, &c.Column_name_bytes, &c.Kind, &c.Position, &c.Type) {
			if c.Kind == "clustering" {
				c.Position++
			}
			if c.Kind == "regular" {
				c.Position += 100000 // works if you have less than this columns
			}
			table.Columns = append(table.Columns, c)
		}
		FatalIfError(iter.Close())
		sort.Sort(ColumnByPosition(table.Columns))
	}
	for _, table := range tables {
		//j, e := json.MarshalIndent(table, "  ", "  ")
		//log.Print(string(j), e)
		fmt.Println("CREATE TABLE", table.Table_name, "(")
		for _, colum := range table.Columns {
			fmt.Print("\t", colum.Column_name, " ", colum.Type)
			fmt.Println(",")
		}
		fmt.Print("\tPRIMARY KEY (")
		kp := 0
		for _, column := range table.Columns {
			if column.Position >= (100000 - 1) {
				continue
			}
			kp++
			if kp != 1 {
				fmt.Print(",")
			}
			fmt.Print(column.Column_name)
		}
		fmt.Println(")")
		fmt.Println(") WITH ")
		WITH := []string{}
		for _, column := range table.Columns {
			if column.Kind != "clustering" {
				continue
			}
			WITH = append(WITH, "CLUSTERING ORDER BY ("+column.Column_name+" "+column.Clustering_order+")")
		}
		WITH = append(WITH, fmt.Sprintf("bloom_filter_fp_chance      = %f", table.Bloom_filter_fp_chance))
		WITH = append(WITH, fmt.Sprintf("crc_check_chance            = %f", table.Crc_check_chance))
		WITH = append(WITH, fmt.Sprintf("dclocal_read_repair_chance  = %f", table.Dclocal_read_repair_chance))
		WITH = append(WITH, fmt.Sprintf("default_time_to_live        = %d", table.Default_time_to_live))
		WITH = append(WITH, fmt.Sprintf("gc_grace_seconds            = %d", table.Gc_grace_seconds))
		WITH = append(WITH, fmt.Sprintf("max_index_interval          = %d", table.Max_index_interval))
		WITH = append(WITH, fmt.Sprintf("memtable_flush_period_in_ms = %d", table.Memtable_flush_period_in_ms))
		WITH = append(WITH, fmt.Sprintf("min_index_interval          = %d", table.Min_index_interval))
		WITH = append(WITH, fmt.Sprintf("read_repair_chance          = %f", table.Read_repair_chance))
		WITH = append(WITH, fmt.Sprintf("speculative_retry           = '%s'", table.Speculative_retry))
		WITH = append(WITH, fmt.Sprintf("caching                     = %s", StringMapToCassandra(table.Caching)))
		WITH = append(WITH, fmt.Sprintf("comment                     = '%s'", table.Comment))
		WITH = append(WITH, fmt.Sprintf("compression                 = %s", StringMapToCassandra(table.Compression)))
		WITH = append(WITH, fmt.Sprintf("compaction                  = %s", StringMapToCassandra(table.Compaction)))
		for k, v := range WITH {
			fmt.Print("\t")
			if k > 0 {
				fmt.Print("AND ")
			} else {
				fmt.Print("    ")
			}
			fmt.Println(v)
		}
		fmt.Println(";")
		fmt.Println("")
	}
	return tables
}

type Type struct {
	Name       string
	FieldNames []string
	FieldTypes []string
}

func dumpKeyspaceTypes(keyspace string) {
	log.Println("-- dumping keyspace", keyspace, "types")
	iter := con.Query(`SELECT 
				type_name,
				field_names,
				field_types 
			FROM system_schema.types 
			WHERE keyspace_name = ?`, keyspace).Iter()
	var t Type
	for iter.Scan(&t.Name, &t.FieldNames, &t.FieldTypes) {
		log.Println("-- dumping keyspace", keyspace, "type", t.Name)
		fmt.Println("CREATE TYPE", t.Name, "(")
		for k, _ := range t.FieldNames {
			fmt.Print("\t", t.FieldNames[k], " ", t.FieldTypes[k])
			if k != (len(t.FieldNames) - 1) {
				fmt.Println(",")
			} else {
				fmt.Println("")
			}
		}
		fmt.Println(");\n")
	}
	FatalIfError(iter.Close())

}

func FatalIfError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func connect() (*gocql.Session, error) {
	cluster := gocql.NewCluster(*cIP)
	cluster.Keyspace = ""
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 3
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}
	cluster.Discovery = gocql.DiscoveryConfig{}
	cluster.NumConns = 1
	cluster.SocketKeepalive = time.Second * 10

	if *cUSER != "" {
		auth := gocql.PasswordAuthenticator{}
		auth.Username = *cUSER
		auth.Password = *cPASS
		cluster.Authenticator = auth
	}

	return cluster.CreateSession()
}

func StringMapToCassandra(in map[string]string) string {
	// we should find a safe way to do this
	j, _ := json.Marshal(in)
	return strings.Replace(string(j), `"`, `'`, -1)
}

func dumpData(keyspace string, tables []*Table) {
	for _, table := range tables {
		dumpDataTable(keyspace, *table)
	}
}

func dumpDataTable(keyspace string, table Table) {
	log.Print("-----------------------------------------------------------------------")
	iter := con.Query("SELECT JSON * from " + keyspace + "." + table.Table_name).Iter()
	var json string
	for iter.Scan(&json) {
		json = strings.Replace(json, "'", "''", -1)
		fmt.Printf("INSERT INTO %s JSON '%s';\n", table.Table_name, json)
	}
	FatalIfError(iter.Close())
}
