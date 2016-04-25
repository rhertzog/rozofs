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

Using the Rozo Console Manager
==============================

RozoFS comes with a command line utility called ``rozo`` that aims to
automate the **management** process of a RozoFS platform. Its main
purpose is to chain up the operations required on remote nodes involved
on a high level management task such as stopping and starting the whole
platform, add new nodes to the platform in order to extend the capacity,
add new exports on volume etc…

``Rozo`` is fully independant of RozoFS daemons and processes and is not
required for a fully functional system but when installed aside RozoFS
on each involved nodes it greatly facilitates configuration as it takes
care of all the unique id generation of storage locations, clusters and
so on. Despite not being a monitoring tool, ``rozo`` can be however used to
get easily a description of the platform, its status and its
configuration.

Rozo uses the running exportd configuration file as a basic platform
knowledge, you can use ``rozo`` on any nodes (even not involve in the
platform).

You can have an overview of ``rozo`` capabilities and get the help you
need by using the rozo manual:

.. code-block:: bash

    $ man rozo

See below, examples of ``rozo`` usage for common management tasks on a 8
nodes platform. Each command is launched on the running exportd node.

Get the List of Nodes Belonging to a Platform
---------------------------------------------

To get informations about all nodes in the platform and their roles.

.. code-block:: bash

    $ rozo node list -E 192.168.1.10
    192.168.1.10:
    - [EXPORTD]
    192.168.1.101:
    - [STORAGED, ROZOFSMOUNT]
    192.168.1.102:
    - [STORAGED, ROZOFSMOUNT]
    192.168.1.103:
    - [STORAGED, ROZOFSMOUNT]
    192.168.1.104:
    - [STORAGED, ROZOFSMOUNT]

You can easily list nodes according to their roles (exportd, storaged or
rozofsmount) using the ``-r`` option.

.. note::
    If you don't want specify the IP address of exportd node for each ``rozo`` 
    command, you can specify the default exportd IP with 
    **ROZO_EXPORT_HOSTNAME** environment variable.

Get the Status of a Platform
----------------------------

To get an overview of the nodes: a RozoFS processes status.

.. code-block:: bash

    $ rozo node status -E 192.168.1.10
    192.168.1.10:
    - {EXPORTD: running}
    192.168.1.101:
    - {STORAGED: running}
    - {ROZOFSMOUNT: running}
    192.168.1.102:
    - {STORAGED: running}
    - {ROZOFSMOUNT: running}
    192.168.1.103:
    - {STORAGED: running}
    - {ROZOFSMOUNT: running}
    192.168.1.104:
    - {STORAGED: running}
    - {ROZOFSMOUNT: running}

You can easily get nodes status according to their roles using the
``-r`` option or get statuses for a specific node using the ``-n``
option.

View the Platform Configuration
-------------------------------

.. code-block:: bash

    $ rozo node config -E 192.168.1.10
    'NODE: 192.168.1.101':
    - STORAGED:
      - INTERFACE:
        - {192.168.1.101: 41001}
      - STORAGE:
        - {'cid 1, sid 1': /srv/rozofs/storages/storage_1_1}
    - ROZOFSMOUNT:
      - {node 192.168.1.10: /srv/rozofs/exports/export_1}
    'NODE: 192.168.1.102':
    - STORAGED:
      - INTERFACE:
        - {192.168.1.102: 41001}
      - STORAGE:
        - {'cid 1, sid 2': /srv/rozofs/storages/storage_1_2}
    - ROZOFSMOUNT:
      - {node 192.168.1.10: /srv/rozofs/exports/export_1}
    'NODE: 192.168.1.103':
    - STORAGED:
      - INTERFACE:
        - {192.168.1.103: 41001}
      - STORAGE:
        - {'cid 1, sid 3': /srv/rozofs/storages/storage_1_3}
    - ROZOFSMOUNT:
      - {node 192.168.1.10: /srv/rozofs/exports/export_1}
    'NODE: 192.168.1.104':
    - STORAGED:
      - INTERFACE:
        - {192.168.1.104: 41001}
      - STORAGE:
        - {'cid 1, sid 4': /srv/rozofs/storages/storage_1_4}
    - ROZOFSMOUNT:
      - {node 192.168.1.10: /srv/rozofs/exports/export_1}
    'NODE: 192.168.1.10':
    - EXPORTD:
      - VOLUME:
        - volume 1:
          - cluster 1:
            - {sid 1: 192.168.1.101}
            - {sid 2: 192.168.1.102}
            - {sid 3: 192.168.1.103}
            - {sid 4: 192.168.1.104}
      - EXPORT:
          vid: 1
          root: /srv/rozofs/exports/export_1
          md5: ''
          squota: ''
          hquota: ''

The output of ``rozo node config`` let us know each node configuration
according to its role. We especially notice that this platform has one
volume with one export relying on it.

Extend the Platform
-------------------

Extend the platform is easy (add nodes) with the ``rozo volume expand``
command, for example purpose we will add four new storages nodes.

.. code-block:: bash

    $ rozo volume expand 192.168.1.201 \
                                     192.168.1.202 \
                                     192.168.1.203 \
                                     192.168.1.204 \
                                     -E 192.168.1.10

As we added nodes without indicating the volume we want to expand,
``rozo`` has created a new volume (with id 2) as illustrated in the
``rozo volume list`` output extract below:

.. code-block:: bash

    $ rozo volume list -E 192.168.1.10
    EXPORTD on 192.168.1.10:
    - VOLUME 1:
      - CLUSTER 1:
        - {STORAGE 1: 192.168.1.101}
        - {STORAGE 2: 192.168.1.102}
        - {STORAGE 3: 192.168.1.103}
        - {STORAGE 4: 192.168.1.104}
    - VOLUME 2:
      - CLUSTER 2:
        - {STORAGE 1: 192.168.1.201}
        - {STORAGE 2: 192.168.1.202}
        - {STORAGE 3: 192.168.1.203}
        - {STORAGE 4: 192.168.1.204}

Add an Export to the Platform
-----------------------------

``rozo export create`` and (``rozo export remove``) commands manage the
creation (and deletion) of new exports.

.. code-block:: bash

    $ rozo export create 2 -E 192.168.1.10

This will create a new export on volume 2.

.. code-block:: bash

    $ rozo export mount -e 2 -E 192.168.1.10

``rozo export mount`` command will configure all nodes with a
rozofsmount role to mount this new export (id=2) as illustrated in the
``df`` output on one of the node.

.. code-block:: bash

    $ df | grep /mnt/rozofs
    rozofs      4867164832      0 4867164832   0% /mnt/rozofs@192.168.1.10/export_1
    rozofs      4867164832      0 4867164832   0% /mnt/rozofs@192.168.1.10/export_2

Rebuild a Storage Node
----------------------

``rozo node rebuild`` command will restart the storaged daemon for one storage 
node with processes for rebuild data of each storage declared in the storaged 
configuration file.

.. code-block:: bash

    $ rozo node rebuild -n 192.168.1.204 -E 192.168.1.10


