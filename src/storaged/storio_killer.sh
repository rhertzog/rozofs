#!/bin/bash

kill_everybody ()
{
  pid_list=""
  for pid in `ps -ef | grep -v grep | grep "storio" | awk '{print $2}'`
  do
    kill $pid
    pid_list=`echo "$pid_list $pid"`
  done
  
  case "$pid_list" in
    "") return;;
  esac   

  sleep $delay
  
  for pid in $pid_list
  do
    kill -9 $pid  > /dev/null 2>&1  
  done   

}
kill_host ()
{
  pid_list=""
  for pid in `ps -ef | grep -v grep | grep "storio" | grep "H $h " | awk '{print $2}'`
  do
    kill $pid
    pid_list=`echo "$pid_list $pid"`
  done
  
  case "$pid_list" in
    "") return;;
  esac  
  
  sleep $delay
  
  for pid in $pid_list
  do
    kill -9 $pid  > /dev/null 2>&1  
  done       
}
kill_instance ()
{
  pid_list=""
  for pid in `ps -ef | grep -v grep | grep "storio" | grep "-i $i" | grep "H $h " | awk '{print $2}'`
  do
    kill $pid
    pid_list=`echo "$pid_list $pid"`
  done
  
  case "$pid_list" in
    "") return;;
  esac  
  
  sleep $delay
  
  for pid in $pid_list
  do
    kill -9 $pid  > /dev/null 2>&1  
  done      
}

while [ ! -z $1 ];
do
  case "$1" in
    -v) set -x; shift 1;;
    -H) h=$2; shift 2;; 
    -i) i=$2; shift 2;;
    *) shift 1;;
  esac
done   

delay="0.2" 


if [  ! -z $h  ] && [ ! -z $i ];
then
  kill_instance 
elif [ ! -z $h ];
then
  kill_host 
else
  kill_everybody 
fi
echo "storio killer $h $i finished"
exit 0
