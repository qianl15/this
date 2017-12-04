#!/bin/bash

./clear.sh
# rm -rf ~/.scanner_db
num=3
fm_nums=("5" "1")
batches=("50" "10")
iters=20

for fm_num in ${fm_nums[@]}; do
    for batch in ${batches[@]}; do
        rm -rf ~/.scanner_db
        out_dir="./results_fused/results_${num}_${fm_num}_${batch}"
        mkdir -p "${out_dir}"
        startfile="start_${num}_${fm_num}_${batch}.txt"
        endfile="end_${num}_${fm_num}_${batch}.txt"
        date > ${startfile}

        for iter in `seq 1 ${iters}`; do
          echo Iteration \# ${iter}
          mkdir -p "${out_dir}/${iter}"
          tmp="fused_iter_${iter}.txt"
          echo "python end2end_fused_mxnet.py  ${num} ${fm_num} \
          ${out_dir}/${iter} ${batch} 1 >> ${tmp}"
          python end2end_fused_mxnet.py  ${num} ${fm_num} \
           "${out_dir}/${iter}" ${batch} 1 >> ${tmp}

          mv ${tmp} "${out_dir}/${iter}"
          sleep 300 # sleep 300 seconds!
        done

        date > ${endfile}
        mv ${startfile} ${out_dir}
        mv ${endfile} ${out_dir}
        sleep 2000 # sleep 2000 seconds!
    done
done
