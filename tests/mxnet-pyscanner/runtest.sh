#!/bin/bash

mkdir -p pymxnet_results
output_dir="./pymxnet_results/"
tmp=`date +"%T.%3N"`.tmp

echo mprof run --multiprocess python2 mxnet_pyscanner.py 5 1 ${output_dir}
mprof run --multiprocess python2 mxnet_pyscanner.py 3 1 ${output_dir} >> ${tmp}
mv *.dat ${output_dir}
