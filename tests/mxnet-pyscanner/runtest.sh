#!/bin/bash

mkdir -p pymxnet_results
output_dir="./pymxnet_results/"

tmp="test_3_5_10.out"
batch=10

echo mprof run --multiprocess python2 mxnet_pyscanner.py 3 5 ${output_dir} ${batch}
mprof run --multiprocess python2 mxnet_pyscanner.py 3 5 ${output_dir} ${batch} >> ${tmp}
mv *.dat ${output_dir}
mv *.out ${output_dir}

sleep 10

tmp="test_3_5_50.out"
batch=50

echo mprof run --multiprocess python2 mxnet_pyscanner.py 3 5 ${output_dir} ${batch}
mprof run --multiprocess python2 mxnet_pyscanner.py 3 5 ${output_dir} ${batch} >> ${tmp}
mv *.dat ${output_dir}
mv *.out ${output_dir}
