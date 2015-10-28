-------------------------------------
Configuring RozoFS using the Rozo CLI
-------------------------------------

About rozo CLI
==============

RozoFS comes with a command line utility called ``rozo`` that aims to
automate the **management** process of a RozoFS platform. Its main
purpose is **to chain up the operations required on remote nodes** involved
on a high level management task such as stopping and starting the whole
platform, add new nodes to the platform in order to extend the capacity,
add new exports on volume etc…

``rozo`` is fully independant of RozoFS daemons and processes and is not
required for a fully functional system but when installed aside RozoFS
on each involved nodes it **greatly facilitates configuration** as it takes
care of all the unique id generation of storage locations, clusters and
so on. Despite not being a monitoring tool, ``rozo`` can be however used to
**get easily a description of the platform, its status and its configuration**.

``rozo`` uses the running exportd configuration ``export.conf`` file as a
basic platform knowledge, you can use ``rozo`` on any nodes 
(even not involve in the platform).

The following diagrams describe the interaction between rozo CLI and the RozoFS
core components.

.. figure:: pics/rozo-CLI-platform.png
   :align: center
   :alt: 

.. figure:: pics/rozo-CLI-in-node.png
   :align: center
   :alt: 


You can have an overview of ``rozo`` capabilities and get the help you
need by using the rozo manual:

.. code-block:: bash

    $ man rozo


Prerequisites
=============

Install rozo manager Packages
-----------------------------

The rozo manager utility consists of 3 packages:

-  ``rozofs-manager-lib``

-  ``rozofs-manager-cli``

-  ``rozofs-manager-agent``

You must install the packages on each node of your platform to use the rozo
utility. If you want to use rozo in a node that is not part of the platform
it is also necessary to install these 3 packages. See 
:doc:`InstallingRozoFS` for get the last packages for your distribution.

Start rozo agent on each node
-----------------------------

The `rozofs-manager-agent` service must be started on each node belonging to the
platform. 

The `rozofs-manager-agent` daemon starts with the following command:

.. code-block:: bash

    $ /etc/init.d/rozofs-manager-agent start
    # OR
    $ service rozofs-manager-agent start

To get the current status of the daemon, the following command is used:

.. code-block:: bash

    $ /etc/init.d/rozofs-manager-agent status
    # OR
    $ service rozofs-manager-agent status


Ensure Connectivity
-------------------

It's necessary that each *rozofs-manager-agent* to be accessible.
It uses the TCP port ``9999``. Ensure that firewall settings do not prevent 
access to this port.

Basic Commands
==============

See below, examples of ``rozo`` usage for common management tasks on a 8
nodes platform. Each command is launched on the running *exportd* node 
(``192.168.1.10``).

Get the List of Nodes Belonging to a Platform
---------------------------------------------

To get informations about all nodes in the platform and their roles.

.. code-block:: bash

    $ rozo node list -E 192.168.1.10
    192.168.1.10:
    - - EXPORTD
    192.168.1.101:
    - - STORAGED
      - ROZOFSMOUNT
    192.168.1.102:
    - - STORAGED
      - ROZOFSMOUNT
    192.168.1.103:
    - - STORAGED
      - ROZOFSMOUNT
    192.168.1.104:
    - - STORAGED
      - ROZOFSMOUNT

You can easily list nodes according to their roles (*exportd*, *storaged* or
*rozofsmount*) using the ``-r`` option.

Get the Status of a Platform
----------------------------

To get an overview of all nodes.

.. code-block:: bash

    $ rozo node status -E 192.168.1.10
    192.168.1.10:
    - EXPORTD: running
    192.168.1.101:
    - STORAGED: running
    - ROZOFSMOUNT: no mountpoint configured
    192.168.1.102:
    - STORAGED: running
    - ROZOFSMOUNT: no mountpoint configured
    192.168.1.103:
    - STORAGED: running
    - ROZOFSMOUNT: no mountpoint configured
    192.168.1.104:
    - STORAGED: running
    - ROZOFSMOUNT: no mountpoint configured

You can easily get nodes status according to their roles using the
``-r`` option or get statuses for a specific node using the ``-n``
option.

.. note::
    If you don't want specify the export IP address each time, it's possible to
    populate the ``ROZO_EXPORT_HOSTNAME`` environment variable.

    *Example*: ``$ export ROZO_EXPORT_HOSTNAME=192.168.1.10``

View the Platform Configuration
-------------------------------

.. code-block:: bash

    $ rozo node config -E 192.168.1.10
    'NODE: 192.168.1.101':
    - STORAGED:
      - INTERFACE:
        - 192.168.1.101: 41001
      - STORAGE:
        - cid 1, sid 1:
            root: /srv/rozofs/storages/storage_1_1
            device-total: 1
            device-mapper: 1
            device-redundancy: 1
    - ROZOFSMOUNT:
      - export host: 192.168.1.10
        export path: /srv/rozofs/exports/export_1
        mountpoint: /mnt/rozofs@192.168.1.10/export_1
    'NODE: 192.168.1.102':
    - STORAGED:
      - INTERFACE:
        - 192.168.1.102: 41001
      - STORAGE:
        - cid 1, sid 2:
            root: /srv/rozofs/storages/storage_1_2
            device-total: 1
            device-mapper: 1
            device-redundancy: 1
    - ROZOFSMOUNT:
      - export host: 192.168.1.10
        export path: /srv/rozofs/exports/export_1
        mountpoint: /mnt/rozofs@192.168.1.10/export_1
    'NODE: 192.168.1.103':
    - STORAGED:
      - INTERFACE:
        - 192.168.1.103: 41001
      - STORAGE:
        - cid 1, sid 3:
            root: /srv/rozofs/storages/storage_1_3
            device-total: 1
            device-mapper: 1
            device-redundancy: 1
    - ROZOFSMOUNT:
      - export host: 192.168.1.10
        export path: /srv/rozofs/exports/export_1
        mountpoint: /mnt/rozofs@192.168.1.10/export_1
    'NODE: 192.168.1.104':
    - STORAGED:
      - INTERFACE:
        - 192.168.1.104: 41001
      - STORAGE:
        - cid 1, sid 4:
            root: /srv/rozofs/storages/storage_1_4
            device-total: 1
            device-mapper: 1
            device-redundancy: 1
    - ROZOFSMOUNT:
      - export host: 192.168.1.10
        export path: /srv/rozofs/exports/export_1
        mountpoint: /mnt/rozofs@192.168.1.10/export_1
    'NODE: 192.168.1.10':
    - EXPORTD:
      - VOLUME:
        - volume 1:
          - cluster 1:
            - sid 1: 192.168.1.101
            - sid 2: 192.168.1.102
            - sid 3: 192.168.1.103
            - sid 4: 192.168.1.104
      - EXPORT:
          vid: 1
          root: /srv/rozofs/exports/export_1
          md5: ''
          squota: ''
          hquota: ''


The output of ``rozo node config`` let us know each node configuration
according to its role. We especially notice that this platform has one
volume with one export relying on it.

Extend the Platform (Add a Volume)
----------------------------------

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
      - LAYOUT: 0
      - CLUSTER 1:
        - STORAGE 1: 192.168.1.101
        - STORAGE 2: 192.168.1.102
        - STORAGE 3: 192.168.1.103
        - STORAGE 4: 192.168.1.104
    - VOLUME 2:
      - LAYOUT: 0
      - CLUSTER 2:
        - STORAGE 1: 192.168.1.201
        - STORAGE 2: 192.168.1.202
        - STORAGE 3: 192.168.1.203
        - STORAGE 4: 192.168.1.204

Add an Export to the Platform
-----------------------------

``rozo export create`` and (``rozo export remove``) commands manage the
creation (and deletion) of new exports.

.. code-block:: bash

    $ rozo export create 2 -E 192.168.1.10

This will create a new export on volume 2.


Mount an Export
---------------

.. code-block:: bash

    $ rozo mount create -e 2 -E 192.168.1.10
    192.168.1.101:
    - export export_2 (eid=2) on /mnt/rozofs@192.168.1.10/export_2:
        configuration: added
        status: mounted
    192.168.1.102:
    - export export_2 (eid=2) on /mnt/rozofs@192.168.1.10/export_2:
        configuration: added
        status: mounted
    192.168.1.103:
    - export export_2 (eid=2) on /mnt/rozofs@192.168.1.10/export_2:
        configuration: added
        status: mounted
    192.168.1.104:
    - export export_2 (eid=2) on /mnt/rozofs@192.168.1.10/export_2:
        configuration: added
        status: mounted

``rozo mount create`` command will configure all nodes with a
*rozofsmount* role to mount this new export (id=2) as illustrated in the
``df`` output on one of the node.

.. code-block:: bash

    $ df | grep /mnt/rozofs
    rozofs      4867164832      0 4867164832   0% /mnt/rozofs@192.168.1.10/export_1
    rozofs      4867164832      0 4867164832   0% /mnt/rozofs@192.168.1.10/export_2

