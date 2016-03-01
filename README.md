# cassdump
cassandra3 dump to cql (experimental)

feel free to provide pull requests!

this tool is not intended for dumping big tables, more for developing and quickly reverting small databases.
it uses a simple `SELECT JSON * FROM ...` which does not work well with big tables, especially when you have many tombstones.
as the default limit of tombstones per select ist 10000, the dump will fail if you have more than that in one table!
