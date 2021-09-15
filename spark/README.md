1. Insert csv file to hadoop fs
```
hadoop fs -mkdir /gerard_tan/spark
cd spark
hadoop fs -put quiz.csv /gerard_tan/spark
```

2. Modify csv filepath and parquet filepath in hadoop

3. Run python file
```
python3 rdd_exercse.py
```