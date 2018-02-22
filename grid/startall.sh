#!/bin/bash


# Issue commands to gfsh to start locator and launch a server
echo "Starting locator and server..."
gfsh <<!
start locator --name=locator1 --port=10334 --properties-file=config/locator.properties --initial-heap=256m --max-heap=256m

start server --name=server1 --server-port=0 --properties-file=config/gemfire.properties --cache-xml-file=./config/cache.xml --initial-heap=1g --max-heap=1g
#start server --name=server2 --server-port=0 --properties-file=config/gemfire.properties --cache-xml-file=./config/cache.xml --initial-heap=1g --max-heap=1g
#start server --name=server3 --server-port=0 --properties-file=config/gemfire.properties --cache-xml-file=./config/cache.xml --initial-heap=1g --max-heap=1g

undeploy --jar=voya-functions-1.jar
deploy --jar=../target/clear-region-function-2.jar

list members;
list regions;
exit;
!
