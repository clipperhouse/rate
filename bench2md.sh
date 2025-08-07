#!/bin/bash

echo "| Name | Iterations | Time | Allocs |"
echo "| :------------- | :--------- | :----------------- | :------------ |"
go test -bench . -benchmem | grep "^Benchmark" | awk '{print "| " $1 " | " $2 " | " $3 " ns/op | " $5 " allocs/op |"}'
