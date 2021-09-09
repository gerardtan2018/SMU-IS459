# How to run deploy hadoop to find all active authors
1. Generate .parquet file on local folder
```
cd hadoop
python3 mongodb_to_parquet.py
```

2. Create directory inside HDFS to store your .parquet file
```
hadoop fs -mkdir /gerard_tan
hadoop fs -mkdir /gerard_tan/active_authors
hadoop fs -mkdir /gerard_tan/active_authors/parquet-input
```

3. Put .parquet file into HDFS
```
hadoop fs -put hardwarezone.parquet /gerard_tan/active_authors/parquet-input
```

4. Package the java file into a .jar file
```
cd ActiveAuthorCount
mvn package
```

5. Run the jar file on hadoop
```
hadoop jar target/AvroParquetAuthorCount-1.0-SNAPSHOT-jar-with-dependencies.jar /gerard_tan/active_authors/parquet-input /gerard_tan/active_authors/parquet-output

hadoop jar target/AvroParquetWordCount-1.0-SNAPSHOT-jar-with-dependencies.jar /gerard_tan/active_authors/parquet-input /gerard_tan/active_authors/parquet-output
```

6. Retrieve out output file
```
hadoop fs -get /gerard_tan/active_authors/parquet-output/part-r-00000
```

#### If need to remove old output file
```
hadoop fs -rm -r /gerard_tan/active_authors/parquet-output
```