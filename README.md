## Performance tests
### Data write performance tests
#### Usage notes
- How data is written: During a test, the test tool automatically increments the IDs in the primary key column, starting from 1 and automatically adding 1 each time you write data. The current time is written to the event time column. Strings with the specified length and primary key values are written to the specified columns of the STRING data type. If the amount of time consumed by data writes reaches the specified time or data has been written to the specified rows, data writes stop, and the written data is calculated.

#### Procedure
Parameters in test_insert.conf:
- testByTime: Specifies whether to perform the test at the specified point in time or based on the specified number of table rows.
- rowNumber: The number of table rows that you want to test. This parameter takes effect only when the value of the testByTime parameter is false.
- testTime: The time when you want to perform the test. Unit: milliseconds. This parameter takes effect only when the value of the testByTime parameter is true.
- tableName: The name of the table used for testing.
- columnCount: The number of table columns. The data type of each column is STRING.
- columnSize: The length of data in each column of the table.
- createTableBeforeRun: Specifies whether to create tables before the test is performed.
- deleteTableAfterDone: Specifies whether to delete the tables after the test is complete.

```
java -jar target/fluss-e2e-performance-tool-1.0-SNAPSHOT.jar test_insert.conf INSERT
```

### Data update performance tests
#### Usage notes

- You must write data to tables and set the deleteTableAfterDone parameter to false to ensure that the tables contain data and can be updated.

#### Procedure

Before the test is performed, you need to only add the writeColumnCount parameter. This parameter specifies the number of table columns where data is written. The columns are of the STRING data type. In this topic, the parameter is set to 50% of the columnCount parameter value.

Execute the following statement to perform a test:
```
java -jar target/fluss-e2e-performance-tool-1.0-SNAPSHOT.jar test_update_part.conf INSERT
```

### Point query performance tests
#### Usage notes
- Description of point query modes:
    - Synchronous point query mode: The point query operation is a blocking operation.
    - Asynchronous point query mode: The point query operation is a non-blocking operation.
- How a point query works: During a test, the test tool randomly generates `IDs` within the configured primary key range for synchronous or asynchronous point queries. If the amount of time consumed by point queries reaches the specified time, point queries stop, and the data used for point queries is calculated.

#### Procedure
The data type of the primary key column is INT or BIGINT. You must make sure that the values in the primary key column are consecutive.

Parameters in test_get.conf:
- keyRangeParams: The primary key range used for point queries. The parameter value is in the format of `<I/L><Start>-<End>`.
- async: The point query mode.

Execute the following statements to perform a test.
```
java -jar target/fluss-e2e-performance-tool-1.0-SNAPSHOT.jar test_get.conf GET
```