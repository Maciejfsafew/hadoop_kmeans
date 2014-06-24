#!/bin/bash

gcc -std=c99 generator.c -o generator.out

mkdir $6


for i in `seq 0 $1 $2`; do
  echo $i
  ./generator.out $i $1 $3 $5 > $6/$7$i
done
