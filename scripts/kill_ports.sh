#!/bin/bash

# Usage: ./kill_ports.sh 7000 9082 9081 ...

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 port1 [port2 port3 ...]"
  exit 1
fi

for port in "$@"
do
  echo "Checking port $port..."
  pid=$(lsof -t -i :$port)

  if [ -z "$pid" ]; then
    echo "No process found using port $port"
  else
    echo "Killing PID(s): $pid"
    kill -9 $pid
    echo "Port $port cleared."
  fi
done