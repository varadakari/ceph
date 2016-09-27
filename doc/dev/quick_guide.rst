=================================
 Developer Guide (Quick)
=================================

This guide will describe how to build and test Ceph for development.

Development
-----------

The ``run-make-check.sh`` script will install Ceph dependencies,
compile everything in debug mode and run a number of tests to verify
the result behaves as expected.

.. code::

       $ ./run-make-check.sh


Running a development deployment
--------------------------------
Ceph contains a script called ``vstart.sh`` (see also :doc:`/dev/dev_cluster_deployement`) which allows developers to quickly test their code using
a simple deployment on your development system. Once the build finishes successfully, start the ceph
deployment using the following command:

.. code::

	$ cd ceph/build  # Assuming this is where you ran cmake
	$ make vstart
	$ ../src/vstart.sh -d -n -x

You can also configure ``vstart.sh`` to use only one monitor and one metadata server by using the following:

.. code::

	$ MON=1 MDS=1 ../src/vstart.sh -d -n -x

The system creates three pools on startup: `cephfs_data`, `cephfs_metadata`, and `rbd`.  Let's get some stats on
the current pools:

.. code::

	$ bin/ceph osd pool stats
	*** DEVELOPER MODE: setting PATH, PYTHONPATH and LD_LIBRARY_PATH ***
	pool rbd id 0
	  nothing is going on

	pool cephfs_data id 1
	  nothing is going on
	
	pool cephfs_metadata id 2
	  nothing is going on
	
	$ bin/ceph osd pool stats cephfs_data
	*** DEVELOPER MODE: setting PATH, PYTHONPATH and LD_LIBRARY_PATH ***
	pool cephfs_data id 1
	  nothing is going on

	$ ./rados df
	pool name       category                 KB      objects       clones     degraded      unfound     rd        rd KB           wr        wr KB
	rbd             -                          0            0            0            0     0            0            0            0            0
	cephfs_data     -                          0            0            0            0     0            0            0            0            0
	cephfs_metadata -                          2           20            0           40     0            0            0           21            8
	  total used        12771536           20
	  total avail     3697045460
	  total space     3709816996


Make a pool and run some benchmarks against it:

.. code::

	$ bin/rados mkpool mypool
	$ bin/rados -p mypool bench 10 write -b 123

Place a file into the new pool:

.. code::

	$ bin/rados -p mypool put objectone <somefile>
	$ bin/rados -p mypool put objecttwo <anotherfile>

List the objects in the pool:

.. code::

	$ bin/rados -p mypool ls

Once you are done, type the following to stop the development ceph deployment:

.. code::

	$ ../src/stop.sh

Resetting your vstart environment
---------------------------------

The vstart script creates out/ and dev/ directories which contain
the cluster's state.  If you want to quickly reset your environment,
you might do something like this:

.. code::

    [build]$ ../src/stop.sh
    [build]$ rm -rf out dev
    [build]$ MDS=1 MON=1 OSD=3 ../src/vstart.sh -n -d

Running a RadosGW development environment
-----------------------------------------
Add the ``-r`` to vstart.sh to enable the RadosGW

.. code::

	$ cd build
	$ ../src/vstart.sh -d -n -x -r

You can now use the swift python client to communicate with the RadosGW.

.. code::

    $ swift -A http://localhost:8000/auth -U tester:testing -K asdf list
    $ swift -A http://localhost:8000/auth -U tester:testing -K asdf upload mycontainer ceph
    $ swift -A http://localhost:8000/auth -U tester:testing -K asdf list


Run unit tests
--------------

The tests are located in `src/tests`.  To run them type:

.. code::

	$ make check

