#!/bin/bash

numExecutor=48
coresPerWorker=1
memExecutor=4G

spark-submit --deploy-mode client --num-executors ${numExecutor} --executor-cores ${coresPerWorker} --executor-memory ${memExecutor} ./FM.py