#!/bin/bash

video_nums=("3")
format_nums=("1" "5")
iter=10

output_dir="./hist_results/"
tmp=`date +"%T.%3N"`.tmp

mkdir -p ${output_dir}

for vnum in ${video_nums[@]}; do
  for fmnum in ${format_nums[@]}; do
    rm -rf "~/.scanner_db" 
    echo mprof run python2 histogram.py ${vnum} ${fmnum} ${output_dir}
    mprof run --multiprocess python2 histogram.py ${iter} ${vnum} ${fmnum} ${output_dir} >> ${tmp}
    mv *.dat ${output_dir}
  done
done


