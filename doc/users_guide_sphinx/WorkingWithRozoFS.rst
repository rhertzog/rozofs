===================
Working with RozoFS
===================

Manual Managing of RozoFS Services
==================================

Starting and Stopping storaged Daemon
-------------------------------------

The storaged daemon starts with the following command:

.. code-block:: bash

    $ /etc/init.d/rozofs-storaged start

To stop the daemon, the following command is used:

.. code-block:: bash

    $ /etc/init.d/rozofs-storaged stop

To get the current status of the daemon, the following command is used:

.. code-block:: bash

    $ /etc/init.d/rozofs-storaged status

To reload the storaged configuration file (``storage.conf``) after a
configuration changes, the following command is used:

.. code-block:: bash

    $ /etc/init.d/rozofs-storaged reload

To automatically start the storaged daemon every time the system boots,
enterone of the following command lines.

For Red Hat based systems:

.. code-block:: bash

    $ chkconfig rozofs-storaged on

For Debian based systems

.. code-block:: bash

    $ update-rc.d rozofs-storaged defaults

Systems Other than Red Hat and Debian:

.. code-block:: bash

    $ echo "storaged" >> /etc/rc.local

Starting and Stopping exportd Daemon
------------------------------------

The exportd daemon is started with the following command:

.. code-block:: bash

    $ /etc/init.d/rozofs-exportd start

To stop the daemon, the following command is used:

.. code-block:: bash

    $ /etc/init.d/rozofs-exportd stop

To get the current status of the daemon, the following command is used:

.. code-block:: bash

    $ /etc/init.d/rozofs-exportd status

To reload the exportd configuration file (``export.conf``) after a
configuration changes, the following command is used:

.. code-block:: bash

    $ /etc/init.d/rozofs-exportd reload

To automatically start the exportd daemon every time the system boots,
enter one of the following command line.

For Red Hat based systems:

.. code-block:: bash

    $ chkconfig rozofs-exportd on

For Debian based systems

.. code-block:: bash

    $ update-rc.d rozofs-exportd defaults

Systems Other than Red Hat and Debian:

.. code-block:: bash

    $ echo "exportd" >> /etc/rc.local

Accessing Data - Setting up rozofsmount Client
----------------------------------------------

After installing the rozofsmount (RozoFS Client), you have to mount the
RozoFS exported file system to access the data. Two methods are
possible: mount manually or automatically.

To manually mount Rozo file system, use the following command:

.. code-block:: bash

    $ rozofsmount -H EXPORT_IP -E EXPORT_PATH MOUNTDIR

For example, if the exported file system is:
``/srv/rozofs/exports/export_1`` and IP address for export server is
192.168.1.10:

.. code-block:: bash

    $ rozofsmount -H 192.168.1.10 -E /srv/rozofs/exports/export_1 /mnt/rozofs/fs-1

To unmount the file system:

.. code-block:: bash

    $ umount /mnt/rozofs/fs-1

To automatically mount a Rozo file system, edit the ``/etc/fstab`` file
and add the following line:

::

    rozofsmount MOUNTDIR rozofs exporthost=EXPORT_IP,exportpath=EXPORT_PATH,_netdev 0  0

For example, if the exported file system is
``/srv/rozofs/exports/export_1`` and IP address for export server is
192.168.1.10 :

::

    rozofsmount /mnt/rozofs/fs1 rozofs exporthost=192.168.1.10,exportpath=/srv/rozofs/exports/export_1,_netdev 0 0

