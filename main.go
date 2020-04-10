package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

var cHELP = flag.Bool("help", false, "show help")
var cIP = flag.String("h", "127.0.0.1", "host or IP of a running cluster node")

var cUSER = flag.String("u", "", "username for the cluster")
var cPASS = flag.String("p", "", "password for the cluster")

var cCertFile = flag.String("cf", "", "CertFile for authentication")
var cKeyFile = flag.String("ck", "", "KeyFile for authentication")

var cEXCLUDE = flag.String("e", "system,system_schema,system_traces,system_distributed", "keyspaces/tables which should not be dumped")
var cINCLUDE = flag.String("i", "", "dump only this keyspaces")

var cConsistency = flag.String("c", "quorum", "Consistency Level: All, Quorum, LocalQuorum")

var con *gocql.Session

func main() {
	flag.Parse()
	if *cHELP {
		flag.PrintDefaults()
		return
	}
	var err error
	con, err = connect(*cIP, *cUSER, *cPASS, *cCertFile, *cKeyFile)
	FatalIfError("connect", err)
	include := StringListToArray(*cINCLUDE)
	exclude := StringListToArray(*cEXCLUDE)
	keyspaces := getKeyspaces(include, exclude)
	ok := true
	for _, keyspace := range keyspaces {
		if !dumpKeyspace(keyspace, include, exclude) {
			ok = false
		}
	}
	if ok == true {
		os.Exit(0)
	} else {
		log.Print("Could not dump all data")
		os.Exit(1)
	}
}

func connect(ip, user, pass, certFile, keyFile string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(ip)
	cluster.Keyspace = ""
	conlevel := strings.ToLower(*cConsistency)
	switch conlevel {
	case "all":
		cluster.Consistency = gocql.All
	case "localquorum":
		cluster.Consistency = gocql.LocalQuorum
	case "quorum":
		cluster.Consistency = gocql.Quorum
	default:
		cluster.Consistency = gocql.Quorum
	}
	title("dumping with Consistency: " + cluster.Consistency.String())
	cluster.ProtoVersion = 4
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{NumRetries: 10, Min: 100 * time.Millisecond, Max: time.Second}
	cluster.SocketKeepalive = time.Second * 10
	cluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	cluster.Timeout = time.Second * 3
	cluster.NumConns = 1
	cluster.SocketKeepalive = time.Second * 10
	cluster.DisableInitialHostLookup = true
	cluster.Events.DisableNodeStatusEvents = true
	cluster.Events.DisableSchemaEvents = true
	cluster.Events.DisableTopologyEvents = true

	if user != "" {
		auth := gocql.PasswordAuthenticator{}
		auth.Username = user
		auth.Password = pass
		cluster.Authenticator = auth
	}
	if certFile != "" {
		if keyFile == "" {
			keyFile = certFile
		}
		cluster.SslOpts = &gocql.SslOptions{CertPath: certFile, KeyPath: keyFile}
	}

	return cluster.CreateSession()
}

func dumpKeyspace(keyspace Keyspace, include []string, exclude []string) bool {
	title("dumping keyspace " + keyspace.Keyspace_name + " at " + time.Now().String())
	fmt.Println("use", keyspace.Keyspace_name, ";\n")
	types := getTypes(keyspace)
	for _, t := range types {
		dumpKeyspaceType(t)
	}
	tables := getTables(keyspace)
	for _, table := range tables {
		dumpKeyspaceTablesDef(keyspace, table)
	}
	ok := true
tl:
	for _, table := range tables {
		for _, et := range exclude {
			if et == keyspace.Keyspace_name+"."+table.Table_name {
				fmt.Print("-- skipping ", keyspace.Keyspace_name+"."+table.Table_name)
				continue tl
			}
		}
		if !dumpDataTable(keyspace, table) {
			ok = false
		}
	}
	return ok
}

func dumpKeyspaceTablesDef(keyspace Keyspace, table Table) {
	for c, column := range table.Columns {
		if column.Kind == "clustering" {
			column.Position++
		}
		if column.Kind == "regular" {
			column.Position += 100000 // works if you have less than this columns
		}
		table.Columns[c] = column
	}
	sort.Sort(ColumnByPosition(table.Columns))
	fmt.Println("CREATE TABLE", table.Table_name, "(")
	for _, column := range table.Columns {
		fmt.Print("\t", column.Column_name, " ", column.Type)
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
	fmt.Print(") WITH ")
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
		if k > 0 {
			fmt.Print("\n\tAND ")
		} else {
			fmt.Print("     ")
		}
		fmt.Print(v)
	}
	fmt.Println(";")
	fmt.Println("")
}

func dumpKeyspaceType(t Type) {
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

func dumpDataTable(keyspace Keyspace, table Table) bool {
	title("dumping " + keyspace.Keyspace_name + "." + table.Table_name)
	iter := con.Query("SELECT JSON * from " + keyspace.Keyspace_name + "." + table.Table_name).Iter()
	var json string
	var count int64
	for iter.Scan(&json) {
		json = strings.Replace(json, "'", "''", -1)
		fmt.Printf("INSERT INTO %s JSON '%s';\n", table.Table_name, json)
		count++
	}
	fmt.Println("-- dumped", count, "rows")
	//FatalIfError("dumpdata", iter.Close())
	if err := iter.Close(); err != nil {
		log.Println("-- ", keyspace.Keyspace_name, ".", table.Table_name, ": ", err.Error(), "\n")
		return false
	}
	fmt.Println()
	return true
}
