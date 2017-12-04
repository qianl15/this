#!/bin/bash

iters=20
out_dir="./results_3_1_50"
mkdir -p "${out_dir}"

for iter in `seq 1 ${iters}`; do
  echo Iteration \# ${iter}
  tmp="iter_${iter}.txt"
  echo python lambda_function.py 6221 0 50
  python lambda_function.py 6221 0 50 >> ${tmp}
  mv ${tmp} ${out_dir}
done

