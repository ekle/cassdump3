package main

type Type struct {
	Name       string
	FieldNames []string
	FieldTypes []string
}

func getTypes(keyspace Keyspace) []Type {
	iter := con.Query(`SELECT
				type_name,
				field_names,
				field_types
			FROM system_schema.types
			WHERE keyspace_name = ?`, keyspace.Keyspace_name).Iter()
	var types []Type
	var t Type
	for iter.Scan(&t.Name, &t.FieldNames, &t.FieldTypes) {
		types = append(types, t)
	}
	FatalIfError("getTypes", iter.Close())
	return types
}
