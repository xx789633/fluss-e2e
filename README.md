## Performance tests
### Data write performance tests
#### Usage notes
- How data is written: During a test, the test tool automatically increments the IDs in the primary key column, starting from 1 and automatically adding 1 each time you write data. The current time is written to the event time column. Strings with the specified length and primary key values are written to the specified columns of the STRING data type. If the amount of time consumed by data writes reaches the specified time or data has been written to the specified rows, data writes stop, and the written data is calculated.

#### Procedure

```
java -jar target/fluss-e2e-performance-tool-1.0-SNAPSHOT.jar test_insert.conf INSERT
```

### Data update performance tests
#### Usage notes

- You must write data to tables and set the deleteTableAfterDone parameter to false to ensure that the tables contain data and can be updated.

#### Procedure

Before the test is performed, you need to only add the writeColumnCount parameter. This parameter specifies the number of table columns where data is written. The columns are of the TEXT data type. In this topic, the parameter is set to 50% of the columnCount parameter value.

Execute the following statement to perform a test:
```
java -jar target/fluss-e2e-performance-tool-1.0-SNAPSHOT.jar test_update_part.conf INSERT
```