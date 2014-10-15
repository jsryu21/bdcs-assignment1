bdcs-assignment1
================

BDCS assignment 1

## Prerequisite
* Install REEF 0.9-SNAPSHOT 
* Install Shimoga 0.1-SNAPSHOT
* Install Wake 0.9-SNAPSHOT
* Install TANG 0.9-SNAPSHOT 
* Set environment parameter YARN_HOME to your hadoop path 

## Parameters 
* input : input file path ( hdfs path or local path ) 
* learning_rate : learning rate ( default value = 0.01)
* lambda : regularization parameter ( default value = 0.001)
* num_iterations: number of iterations ( default value = 10)

## Dataset 
* Skin segmentation (http://archive.ics.uci.edu/ml/datasets/Skin+Segmentation)
* You <b>must</b> use this dataset for input. 

## How to run? 
* Add the input file to hdfs 
* mvn clean install 
* ./start.sh $parameters$ 
* Ex) ./start.sh --input=$HDFS_INPUT_PATH$ --learning_rate=0.01


## Reference
* Parallelized SGD (http://cs.markusweimer.com/2010/12/09/parallelized-stochastic-gradient-descent/)
* A Convenient Framework for Efﬁcient Parallel Multipass Algorithms (http://cs.markusweimer.com/2010/12/11/a-convenient-framework-for-ef%EF%AC%81cient-parallel-multipass-algorithms/)
