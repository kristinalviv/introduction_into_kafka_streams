***Prerequisite***:
 - docker must be installed

**How to run:**  
1. Run kafka cluster: 
```
docker compose -f init-kafka-cluster.yaml up
```
*Cluster is created with 3 nodes and one 8 partition topic.*

2. Run kafka streaming application that calculates a visiting statistic 
```
docker compose -f run-browser-statistics.yaml up
```
3. To finish the work run
```
docker compose -f run-browser-statistics.yaml -f init-kafka-cluster.yaml down
```