#!/bin/sh

while read LINE
do
	python ncbiupdate.py -a 192.168.40.81:29025 -i '/mnt/daily-nc/2013/'${LINE}
done < $1
