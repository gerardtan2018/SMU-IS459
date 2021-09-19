## Assignment 2
### There are 2 ways to run assignment 2. First is by running the python file as shown in the steps below. Second is by opening up assignment_2.ipynb and run the cells from there. The python assignment_2.py will consist of more in depth comments about the code.

1. Insert csv file to hadoop fs
```
hadoop fs -mkdir /gerard_tan/spark
cd hadoop
hadoop fs -put hardwarezone.parquet /gerard_tan/spark
```

2. Before running the python file, ensure that al the necessary packages are installed (e.g. pyspark)

3. Run python file
```
python3 rdd_exercse.py
```