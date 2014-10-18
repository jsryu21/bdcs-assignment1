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
* I already added the dataset to datset/Skin_NonSkin.txt 

## How to run? 
* Add the input file to hdfs 
* mvn clean install 
* ./start.sh $parameters$ 
* Ex) ./start.sh --input=$HDFS_INPUT_PATH$ --learning_rate=0.01
* You can see the result from your console. 
```
================== Result ===============
Iteration 0 Parameters : [0.00 0.00 0.00 0.00 ]
Iteration 0 loss: 61579.63
Iteration 1 Parameters : [0.48 0.17 0.20 0.10 ]
Iteration 1 loss: 6455.49
Iteration 2 Parameters : [0.50 0.06 0.08 0.06 ]
Iteration 2 loss: 6595.40
Iteration 3 Parameters : [0.50 0.05 0.07 0.04 ]
Iteration 3 loss: 6633.65
Iteration 4 Parameters : [0.50 0.06 0.06 0.04 ]
Iteration 4 loss: 6634.99
Iteration 5 Parameters : [0.50 0.05 0.06 0.04 ]
Iteration 5 loss: 6620.14
Iteration 6 Parameters : [0.50 0.05 0.07 0.05 ]
Iteration 6 loss: 6628.00
Iteration 7 Parameters : [0.50 0.05 0.07 0.04 ]
Iteration 7 loss: 6637.41
Iteration 8 Parameters : [0.49 0.05 0.06 0.06 ]
Iteration 8 loss: 6640.44
Iteration 9 Parameters : [0.50 0.05 0.07 0.04 ]
Iteration 9 loss: 6617.02
Iteration 10 Parameters : [0.49 0.05 0.07 0.04 ]
Iteration 10 loss: 6359.94

=========================================
```
The loss value decreases as it iterates the computation. 

## Reference
* Parallelized SGD (http://cs.markusweimer.com/2010/12/09/parallelized-stochastic-gradient-descent/)
* A Convenient Framework for Efﬁcient Parallel Multipass Algorithms (http://cs.markusweimer.com/2010/12/11/a-convenient-framework-for-ef%EF%AC%81cient-parallel-multipass-algorithms/)
