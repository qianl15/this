#!/bin/bash

./clear.sh
rm -rf ~/.scanner_db
num=3
fm_num=1
batches=("10")
iters=20

for batch in ${batches[@]}; do
    out_dir="./results/results_${num}_${fm_num}_${batch}"
    mkdir -p "${out_dir}"

    for iter in `seq 1 ${iters}`; do
      echo Iteration \# ${iter}
      tmp="iter_${iter}.txt"
      echo python end2end_mxnet.py ${num} ${fm_num} ./ ${batch} 1 >> ${tmp}
      python end2end_mxnet.py ${num} ${fm_num} ./ ${batch} 1 >> ${tmp}
      mkdir -p "${out_dir}/${iter}"
      mv *.out "${out_dir}/${iter}"
      mv ${tmp} "${out_dir}/${iter}"
      sleep 300 # sleep 300 second!
    done
done
