# CS 582 Project
Group 9

# Building 
``` bash
cd src
make
```

# Testing
``` bash
cd src
make test
```

# Running
## Batch Insert (Creating Database files from text)
You can create a database with the following commands
``` bash
cd src
make
java tools.batchinsert [input file] [db name] [column|row] [table name] [num columns]
```
**Note** The number of columns need to match the input file

## Query (Running simple queries on existing database files)
You can query a database with the following commands
``` bash
cd src
make
java tools.query [db name] [table name] [attribute] "[operator]" [value] [buffer size]
```
**NOTE**: operators must be wrapped in quotes

Valid operators are: =, !=, >, >=, <, and <=

**NOTE**: buffer size should be >= 8 for normal use

## QSort (Running simple sort queries on existing database files)
You can qsort a database with the following commands
``` bash
cd src
make
java tools.qsort [db name] [table name] [attribute] size]
```

**NOTE**: buffer size should be >= 8 for normal use

