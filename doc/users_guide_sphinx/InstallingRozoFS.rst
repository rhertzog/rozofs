-----------------
Installing RozoFS
-----------------

Installing RozoFS from Binary Packages
======================================

Fizians SAS provides **binary packages** for every component of RozoFS
and various GNU/Linux distributions based on Debian (``.deb``) and
Redhat (``.rpm``) package format. Using binary packages brings you
benefits. First, you do not need a full development environment and
other hand binary packages come with init script, easy dependency
management etc... that can simplify deployment and management process.
See help of your favorite GNU/Linux distribution's package manager for
package installation. According to their roles, nodes should have at
least one these packages installed :

-  ``rozofs-storaged_<version>_<arch>.<deb|rpm>``

-  ``rozofs-exportd_<version>_<arch>.<deb|rpm>``

-  ``rozofs-rozofsmount_<version>_<arch>.<deb|rpm>``

To help and automate management, the following optional packages should
be installed on each node involved in a RozoFS platform:

-  ``rozofs-manager-lib_<version>_<arch>.<deb|rpm>``

-  ``rozofs-manager-cli_<version>_<arch>.<deb|rpm>``

-  ``rozofs-manager-agent_<version>_<arch>.<deb|rpm>``

-  ``rozofs-rprof_<version>_<arch>.<deb|rpm>``

-  ``rozofs-rozodiag_<version>_<arch>.<deb|rpm>``

Advance Package Tool (APT) for Debian Wheezy
--------------------------------------------

Install Release Key
~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    $ wget -O - http://dl.rozofs.org/debian/devel@rozofs.com.gpg.key | apt-key add -

Add Release Packages
~~~~~~~~~~~~~~~~~~~~

For the lastest stable release :

.. code-block:: bash

    $ echo deb http://dl.rozofs.org/debian/master $(lsb_release -sc) main | tee /etc/apt/sources.list.d/rozofs.list

For the lastest development release :

.. code-block:: bash

    $ echo deb http://dl.rozofs.org/debian/develop $(lsb_release -sc) main | tee /etc/apt/sources.list.d/rozofs.list

Install Packages
~~~~~~~~~~~~~~~~

On each node involved in a RozoFS platform:

.. code-block:: bash

    $ apt-get update
    $ apt-get install rozofs-storaged
    $ apt-get install rozofs-exportd
    $ apt-get install rozofs-rozofsmount
    $ apt-get install rozofs-rprof
    $ apt-get install rozofs-rozodiag

To help and automate management:

.. code-block:: bash

    $ apt-get install rozofs-manager-lib
    $ apt-get install rozofs-manager-cli
    $ apt-get install rozofs-manager-agent

Yum for CentOS 7
----------------

Install repository Package
~~~~~~~~~~~~~~~~~~~~~~~~~~

For the lastest stable release :

.. code-block:: bash

    $ yum install http://dl.rozofs.org/rpms/master/el7/noarch/repo-rozofs-master-1.0-1.el7.centos.noarch.rpm

For the lastest development release :

.. code-block:: bash

    $ yum install http://dl.rozofs.org/rpms/develop/el7/noarch/repo-rozofs-master-1.0-1.el7.centos.noarch.rpm

Install Packages
~~~~~~~~~~~~~~~~

On each node involved in a RozoFS platform:

.. code-block:: bash

    $ yum install rozofs-storaged
    $ yum install rozofs-exportd
    $ yum install rozofs-rozofsmount
    $ yum install rozofs-rprof
    $ yum install rozofs-rozodiag

To help and automate management:

.. code-block:: bash

    $ yum install rozofs-manager-lib
    $ yum install rozofs-manager-cli
    $ yum install rozofs-manager-agent


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

