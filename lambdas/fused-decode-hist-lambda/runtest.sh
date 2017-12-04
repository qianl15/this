#!/bin/bash

iters=20
num=3
fm_nums=("5" "1")
batches=("50" "10")

for fm_num in ${fm_nums[@]}; do
  for batch in ${batches[@]}; do
    rm -rf ~/.scanner_db
    out_dir="./results_local_hist/results_${num}_${fm_num}_${batch}"
    mkdir -p ${out_dir}
    for iter in `seq 1 ${iters}`; do
      echo Iteration \# ${iter}
      tmp="iter_${iter}.txt"
      echo python lambda.py 6221 0 ${fm_num} ${batch}
      python lambda.py 6221 0 ${fm_num} ${batch} >> ${tmp}
      mv ${tmp} ${out_dir}
    done
  done
done

rm /tmp/FusedDecodeHist-static
