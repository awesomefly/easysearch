#!/bin/sh
CURDIR=$(cd $(dirname $0); pwd)
echo "$CURDIR"

BIN=easysearch

if [ $1 = "standalone" ]; then
  ps -ef|grep $BIN|grep -v "grep"|awk -F " " '{print $2}'|xargs kill -9
  sleep 1

  ./$BIN -m cluster --servername=all >> ${CURDIR}/err.log 2>&1 &
  echo "started standalone cluster "
elif [ $1 = "manager" ]; then
  ./$BIN -m cluster --servername=managerserver >> ${CURDIR}/err.log 2>&1 &
elif [ $1 = "data" ]; then
  ./$BIN -m cluster --servername=dataserver >> ${CURDIR}/err.log 2>&1 &
elif [ $1 = "search" ]; then
  ./$BIN -m cluster --servername=searchserver >> ${CURDIR}/err.log 2>&1 &
elif [ $1 = "kill" ]; then
  ps -ef|grep $BIN|grep -v "grep"|awk -F " " '{print $2}'|xargs kill -9
fi



