#!/bin/bash

pushd cmake/build/
make -j 4
cp ecloud_server ../../
popd