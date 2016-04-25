-----------------
Installing RozoFS
-----------------

Installing RozoFS from Binary Packages
======================================

Rozo Systems SAS provides **binary packages** for every component of RozoFS
and various GNU/Linux distributions based on Debian (``.deb``) and
Redhat (``.rpm``) package format. Using binary packages brings you
benefits. First, you do not need a full development environment and
other hand binary packages come with init script, easy dependency
management etc... that can simplify deployment and management process.
See help of your favorite GNU/Linux distribution's package manager for
package installation. According to their roles, nodes should have at
least one these packages installed :

-  ``rozofs-storaged``

-  ``rozofs-exportd``

-  ``rozofs-rozofsmount``

To help and automate management, the following optional packages should
be installed on each node involved in a RozoFS platform:

-  ``rozofs-manager-lib``

-  ``rozofs-manager-cli``

-  ``rozofs-manager-agent``

To monitor and to get statistics about each RozoFS component,
the following optional packages should be installed on each node involved in a RozoFS platform:

-  ``rozofs-rozodiag``

-  ``nagios-plugins-rozofs``


Advance Package Tool (APT) for Debian/Ubuntu
--------------------------------------------

Tested on the following OS:\

-  Debian 7 (*wheezy*), 8 (*jessie*)
-  Ubuntu 14.04 (*trusty*)


Install Release Key
~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    $ wget -O - http://dl.rozofs.org/deb/devel@rozofs.com.gpg.key | apt-key add -

Add the repository
~~~~~~~~~~~~~~~~~~

For the lastest stable release:

.. code-block:: bash

    $ echo deb http://dl.rozofs.org/deb/master $(lsb_release -sc) main | tee /etc/apt/sources.list.d/rozofs.list

For the lastest development release:

.. code-block:: bash

    $ echo deb http://dl.rozofs.org/deb/develop $(lsb_release -sc) main | tee /etc/apt/sources.list.d/rozofs.list

Install Packages
~~~~~~~~~~~~~~~~

On each node involved in a RozoFS platform:

.. code-block:: bash

    $ apt-get update
    $ apt-get install rozofs-storaged
    $ apt-get install rozofs-exportd
    $ apt-get install rozofs-rozofsmount


To help and automate management:

.. code-block:: bash

    $ apt-get install rozofs-manager-lib
    $ apt-get install rozofs-manager-cli
    $ apt-get install rozofs-manager-agent

To monitor and to get statistics:

.. code-block:: bash

    $ apt-get install rozofs-rozodiag
    $ apt-get install nagios-plugins-rozofs

Yum for CentOS (RHEL)
---------------------

Tested on the following OS:\

-  CentOS 6 (*el6*), 7 (*el7*)


Install repository package
~~~~~~~~~~~~~~~~~~~~~~~~~~

For the lastest stable release on *el7*:

.. code-block:: bash

    $ yum install http://dl.rozofs.org/rpms/master/el7/noarch/repo-rozofs-master-1.0-1.el7.noarch.rpm

For the lastest development release on *el7*:

.. code-block:: bash

    $ yum install http://dl.rozofs.org/rpms/develop/el7/noarch/repo-rozofs-develop-1.0-1.el7.noarch.rpm

Install Packages
~~~~~~~~~~~~~~~~

On each node involved in a RozoFS platform:

.. code-block:: bash

    $ yum install rozofs-storaged
    $ yum install rozofs-exportd
    $ yum install rozofs-rozofsmount

To help and automate management:

.. code-block:: bash

    $ yum install rozofs-manager-lib
    $ yum install rozofs-manager-cli
    $ yum install rozofs-manager-agent

To monitor and to get statistics:

.. code-block:: bash

    $ yum install rozofs-rozodiag


Building and Installing from Sources
====================================

Prerequisites
-------------

The latest stable release of RozoFS can be downloaded from
`http://github.com/rozofs/rozofs <http://github.com/rozofs/rozofs>`_.

To build the RozoFS source code, it is necessary to install several
libraries and tools. RozoFS uses the cross-platform build system
**cmake** to get you started quickly. RozoFS **dependencies** are the
following:

-  ``cmake``

-  ``libattr1-dev``

-  ``uuid-dev``

-  ``libconfig-dev``

-  ``libfuse-dev``

-  ``libreadline-dev``

-  ``python2.7-dev``

-  ``libpthread``

-  ``libcrypt``

-  ``swig``

Build the Source
----------------

Once the required packages are installed on your appropriate system, you
can generate the build configuration with the following commands (using
default values compiles RozoFS in Release mode and installs it on
``/usr/local``) :

.. code-block:: bash

    $ cmake -G "Unix Makefiles" ..

    -- The C compiler identification is GNU
    -- Check for working C compiler: /usr/bin/gcc
    -- Check for working C caompiler: /usr/bin/gcc -- works
    -- Detecting C compiler ABI info
    -- Detecting C compiler ABI info - done
    -- Configuring done
    -- Generating done
    -- Build files have been written to: /root/rozofs/build
    $ make
    $ make install

If you use default values, make will place the executables in
``/usr/local/bin``, build options (CMAKE\_INSTALL\_PREFIX,
CMAKE\_BUILD\_TYPE...) of generated build tree can be modified with the
following command :

.. code-block:: bash

    $ make edit_cache

