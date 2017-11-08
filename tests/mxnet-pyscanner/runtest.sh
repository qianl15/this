#!/bin/bash

mkdir -p pymxnet_results
output_dir="./pymxnet_results/"
tmp="test_3_1.out"

echo mprof run --multiprocess python2 mxnet_pyscanner.py 3 1 ${output_dir}
mprof run --multiprocess python2 mxnet_pyscanner.py 3 1 ${output_dir} >> ${tmp}
mv *.dat ${output_dir}
mv *.out ${output_dir}
