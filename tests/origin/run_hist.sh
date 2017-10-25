#!/bin/bash

video_nums=("1" "2")
format_nums=("1" "2")
format_nums2=("1" "2" "3" "4")

mkdir -p hist_results

output_dir="./hist_results/"
tmp=`date +"%T.%3N"`.tmp

for vnum in ${video_nums[@]}; do
  for fmnum in ${format_nums[@]}; do
    echo mprof run python2 histogram.py ${vnum} ${fmnum} ${output_dir}
    mprof run --multiprocess python2 histogram.py ${vnum} ${fmnum} ${output_dir} >> ${tmp}
    mv *.dat ${output_dir}
  done
done


