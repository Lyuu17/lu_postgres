#!/bin/sh
rm -rf build;
mkdir build && cd build && cmake .. -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release -DVCPKG_TARGET_TRIPLET=x86-linux -DCMAKE_C_FLAGS="-m32" -DCMAKE_CXX_FLAGS="-m32" && make && make install && cd ..;
