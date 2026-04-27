using BigQuery.InMemoryEmulator;
using BigQuery.InMemoryEmulator.SqlEngine;
var store = new InMemoryDataStore("test-project");
store.Datasets["ds"] = new InMemoryDataset("ds");
var exec = new QueryExecutor(store, "ds");
var r = exec.Execute("SELECT BOOL(PARSE_JSON('true')) AS val");
var val = r.Rows[0].F[0].V;
Console.WriteLine($"Value: {val}, Type: {val?.GetType()}");
