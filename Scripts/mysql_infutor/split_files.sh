#!/bin/bash
for file in raw/*.txt
do
	name=${file##*/}
	base=${name%.txt}
	echo "${file}"
	split -l 200000 -d "${file}" raw_chunked/"${base}"
done

for file in raw_chunked/*
do
	echo "${file}"
	mv "${file}" "${file}".txt
done