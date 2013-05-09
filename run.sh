#!/bin/sh

while read LINE
do
#        num=`gzip -dc '/mnt/'${LINE} | wc -l`
#        echo $num' ./'${LINE}
	python ncbirdins2.py -a 192.168.40.81:29025 -i '/mnt/genbank130419/'${LINE}
done < $1
