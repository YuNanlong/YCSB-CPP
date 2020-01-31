#!/bin/sh

do_cmake () {
  build_type=$1
  gcc_flag="-g -lrt -ljemalloc"
  if [ $build_type == "Debug" ]
  then
    gcc_flag="$gcc_flag -fPIC"
  else
    gcc_flag="$gcc_flag -O2 -DNDEBUG -fPIC"
  fi
  
  \rm -rf ./CMakeFiles
  \rm -f ./CMakeCache.txt

  cmake . -DCMAKE_C_FLAGS="$gcc_flag" \
          -DCMAKE_CXX_FLAGS="$gcc_flag" \
          -DCMAKE_BUILD_TYPE=$build_type \
          -DUSE_RTTI=YES
}

export CC=/usr/local/gcc-4.9.2/bin/gcc
export CXX=/usr/local/gcc-4.9.2/bin/g++

if [ $1 == "b" ]
then
  do_cmake Release
  make -j
elif [ $1 == "d" ]
then
  do_cmake Debug
  make -j
fi