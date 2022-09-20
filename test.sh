#!/bin/bash


task="taskset -c 1"
out="out.txt"


rm /mnt/pmem/lgc/*
for i in {1..4}
do
    ${task} ./main -c ${i} >> ${out}
done


