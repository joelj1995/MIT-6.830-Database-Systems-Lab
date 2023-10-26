# Testing

### Run all unit tests

`ant test`

### Run a specific unit test

`ant runtest -Dtest=TupleTest`

### Run systems tests

`ant systemtest`

# Use the Query Parser

Create a text file `data.txt` with your data.

```
1,10
2,20
3,30
4,40
5,50
5,50
```

Build the code.

`ant dist`

Create a data file.

`java -jar .\dist\simpledb.jar convert data.txt 2 "int,int"`

Create the catalog file `catalog.txt`.

```
data (f1 int, f2 int)
```

This defines a table `data` with two integer fields `f1` and `f2`.

Invoke the parser.

`java -jar dist/simpledb.jar parser catalog.txt`

Now you can run queries against the interactive prompt.

```
SimpleDB> select d.f1, d.f2 from data d;
```