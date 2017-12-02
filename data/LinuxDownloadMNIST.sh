#!/usr/bin/env bash

wget http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/mnist.scale.bz2
bzip2 -d mnist.scale.bz2
mv mnist.scale mnist


wget https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/mnist8m.scale.bz2
bzip2 -d mnist8m.scale.bz2
mv mnist8m.scale mnist8m