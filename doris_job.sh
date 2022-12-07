#!/bin/bash
. /etc/profile
echo $1

python /data/datax/bin/datax.py  --jvm="-Xms3G -Xmx3G" $1