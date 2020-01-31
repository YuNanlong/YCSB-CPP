#!/bin/bash

if [ $# -eq 0 ]
then
  echo ERROR
  exit 1
fi

ROCKSDB_DIR=$1

cp tools/* $ROCKSDB_DIR/tools
cp util/* $ROCKSDB_DIR/util
cp include/rocksdb/* $ROCKSDB_DIR/include/rocksdb
mv CMakeLists.txt $ROCKSDB_DIR