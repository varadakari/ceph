#!/bin/sh -x
git submodule update --init --recursive
if test -e build; then
    echo 'build dir already exists; rm -rf build and re-run'
    exit 1
fi
mkdir build
cd build
cmake -D ALLOCATOR=jemalloc $@ ..

cat <<EOF > ceph.conf
plugin dir = lib
erasure code dir = lib
EOF

echo done.
