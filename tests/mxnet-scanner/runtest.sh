#!/bin/bash

mkdir -p mxnet_results
output_dir="./mxnet_results/"

batches=("50" "40" "30" "20" "10" "1")
#batches=("30" "20" "10" "1")
#batches=("50" "40")

num=3
fmnums=("1" "2" "3")

for fmnum in ${fmnums[@]}; do
  for batch in ${batches[@]}; do
    tmp="mxnetlambda_${num}_${fmnum}_${batch}.out"
    echo mprof run --multiprocess python2 mxnet_lambda.py 15iuxppcah.execute-api.us-west-2.amazonaws.com /prod/mxnet-test-dev-hello ${batch} ${num} ${fmnum} ${output_dir}
    mprof run --multiprocess python mxnet_lambda.py 15iuxppcah.execute-api.us-west-2.amazonaws.com /prod/mxnet-test-dev-hello ${batch} ${num} ${fmnum} ${output_dir}  >> ${tmp}
    mv *.dat ${output_dir}
    mv *.out ${output_dir}

  done
done
