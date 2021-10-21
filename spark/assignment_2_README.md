## Assignment 2
### There are 2 ways to run assignment 2. First is by running the python file as shown in the steps below. Second is by opening up assignment_2.ipynb and run the cells from there. The python assignment_2.py will consist of more in depth comments about the code.

1. Start hadoop with the following commands. May differ from system to system depending on location of folder and version.
```
sudo service ssh start
cd ~/hadoop/hadoop-3.3.0
sbin/start-dfs.sh
sbin/start-yarn.sh
```

2. Insert parquet file to hadoop fs
```
hadoop fs -mkdir /gerard_tan/spark
cd hadoop
hadoop fs -put hardwarezone.parquet /gerard_tan/spark
```

3. Before running the python file, ensure that all the necessary packages are installed (e.g. pyspark). If not you can create an environment and install the necessary packages from the base folder.
```
# Run the line below if you are still in the "hadoop" folder
cd ..

# From "/SMU-IS459" folder run the line below
virtualenv .env2 && source .env2/bin/activate && pip install -r assignment2_requirements.txt
```

4. Run python file (If running from environment, you can use the same terminal as the one in the previous step where the environment has already been activated)
```
python3 rdd_exercse.py
```