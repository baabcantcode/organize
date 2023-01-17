# csv_transform

## running this from CLI to generate a new CSV from an old one
``` sh
csv_transform -- -f testin.csv "select * from table1 where big = 'feels'" testout.csv
```


## overview

Use this cli to run sql queries to transform 1+ input CSV files into a result CSV.
This tool runs on sqlite, and you can refer to he standard sqlite docs for query rules.
example usage with a single CSV input at ./thiscsv.csv

csv_transform -f thiscsv.csv "select * from table1 where id = '235'" out.csv

the output argument is optional, if it's skipped this will print to stdout 
(not guaranteed to follow CSV format when printed to stdout)

the initial table created from the csv will be named 'table1' and additional tables
will be named in the order that they were given to this cli -> table2, table3 etc

the out.csv is a user defined file path which will be written to based on the user's query
