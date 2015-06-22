#!/bin/bash 
echo "$0 `date`" >> .last-make
echo BUILDING FOR UBUNTU 12.04
export CC=gcc
export CXX=g++

export LD_LIBRARY_PATH=$HOME/opt/ovis/lib:$LD_LIBRARY_PATH

# local path of scratch ldms files
build_subdir=LDMS_objdir

if test -f lib/packaging/ovis-lib-toss.spec.in; then
	prefix=$HOME/opt/ovis
	expected_event2_prefix=/usr
	expected_ovislib_prefix=$prefix
	expected_sos_prefix=$prefix

#CFLAGS='-Wall -g'
CXXFLAGS='-Wall -g -I/ovis/init-2015/include'

	if test -f $expected_event2_prefix/include/event2/event.h; then
		echo "Found $expected_event2_prefix/include/event2/event.h. Good."
	else
		echo "You forgot to install libevent -dev package or you need to edit $0"
		exit 1
	fi
	if test -f ldms/configure; then
		echo "Found ldms/configure. Good."
	else
		echo "You forgot to autogen.sh at the top or you need to edit $0 or you need to use
 a released tarred version."
		exit 1
	fi

	srctop=`pwd`
	prefix=$srctop/LDMS_install
	echo "reinitializing build subdirectory $build_subdir" 
	rm -rf $build_subdir
	mkdir $build_subdir
	cd $build_subdir
	expected_ovislib_prefix=$prefix
	expected_sos_prefix=/badsos
	#allconfig="--prefix=$prefix --enable-rdma --enable-ssl --with-libevent=$expected_event2_prefix --disable-sos --disable-perfevent --disable-zap --disable-zaptest --disable-swig --enable-authentication --enable-libgenders --with-libgenders=$HOME/ovis/init-2015 LDFLAGS=-fsanitize=address "
	allconfig="--prefix=$prefix --enable-rdma --enable-ssl --with-libevent=$expected_event2_prefix --disable-sos --disable-perfevent --disable-zap --disable-zaptest --disable-swig --enable-authentication --enable-libgenders --with-libgenders=$HOME/ovis/init-2015 "
	../configure $allconfig && \
	make && \
	make install && \
	../packaging/nola.sh $prefix
else
	echo "this must be run from the top of ovis source tree"
	exit 1
fi
