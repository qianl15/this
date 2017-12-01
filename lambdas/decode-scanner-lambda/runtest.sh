#!/bin/bash

iters=20
out_dir="./results_3_5_50"
mkdir -p "${out_dir}"

for iter in `seq 1 ${iters}`; do
  echo Iteration \# ${iter}
  tmp="iter_${iter}.txt"
  echo python lambda.py
  python lambda.py >> ${tmp}
  mv ${tmp} ${out_dir}
done

rm /tmp/DecoderAutomataCmd-static
