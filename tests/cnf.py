#!/usr/bin/python
# -*- coding: utf-8 -*-
cnf_clusters=[]
global layout
global layout_int
global nbclusters
global clients_nb
global georep

#_____________________________________ 
def setLayout(l=0):
  global layout
  global layout_int
  
  layout_int=int(l)
  if   l == 0: layout = rozofs.layout_2_3_4()
  elif l == 1: layout = rozofs.layout_4_6_8()
  elif l == 2: layout = rozofs.layout_8_12_16()
  else: 
    print "No such layout %s"%(l)
    sys.exit(-1)  
  
#_____________________________________ 
def setVolumeHosts(nbHosts):
  global layout_int
  global nbclusters
  global clients_nb
  global georep
  global failures
    
  # Is there more server than sid per cluster
  factor=int(1)
  safe=rozofs.min_sid(layout_int)
  nb=int(nbHosts)
  while int(safe) > int(nb):
    factor=int(factor) * int(2)
    nb=int(nb) * int(2)  
    
  # Create a volume
  v1 = volume_class(layout)
  if factor != 1:
    failures = int(v1.get_failures())
    failures = failures / factor
    v1.set_failures(failures)
  
  # Create clusters on this volume
  for i in range(nbclusters):
    c = v1.add_cid(devices,mapper,redundancy)  
    cnf_clusters.append(c)

    # Create the required number of sid on each cluster
    # The 2 clusters use the same host for a given sid number
    for s in range(nbHosts):
      if georep == False:
        for f in range(factor):
          c.add_sid_on_host(s+1,s % rozofs.site_number)
      else:
        # In geo replication 
	# host2 on site 1 replicates host1 on site 0
	# host4 on site 1 replicates host3 on site 0...	
        for f in range(factor):
          c.add_sid_on_host((2*s)+1,0,(2*s)+2,1)
        
  return v1  
    
#_____________________________________ 
def addExport(vol,layout=None):

  # Create on export for 4K, and one mount point
  e = vol.add_export(rozofs.bsize4K(),layout)

  for i in range(1,clients_nb+1): 
    m1 = e.add_mount((i-1) % rozofs.site_number)
    if georep==True: m2 = e1.add_mount(1)

    
#_____________________________________ 
georep = False
#georep = True

# Number of sites
#rozofs.set_site_number(4)

#rozofs.set_trace()

rozofs.set_alloc_mb(0);

# Change number of core files
# rozofs.set_nb_core_file(1);

# Minimum delay in sec between remove and effective deletion
rozofs.set_deletion_delay(12)

# Enable FID recycling
#rozofs.set_fid_recycle(10)
#--------------STORIO GENERAL

# Set original RozoFS file distribution
rozofs.set_file_distribution(2)

# Set single storio mode
# rozofs.storio_mode_single()

# Disable CRC32
# rozofs.set_crc32(False)

# Disable self healing
rozofs.set_self_healing(1)

# Modify number of listen port/ per storio
# rozofs.set_nb_listen(4)

# Modify number of storio threads
# rozofs.set_threads(8)

# Use fixed size file mounted through losetup for devices
#rozofs.set_ext4(100)
#rozofs.set_xfs(1000,None)
#rozofs.set_xfs(1000,"4096")
#rozofs.set_xfs(1000,"64K")
#rozofs.set_xfs(1000,"128M")

#--------------CLIENT GENERAL

# Enable mojette thread for read
# rozofs.enable_read_mojette_threads()

# Disable mojette thread for write
# rozofs.disable_write_mojette_threads()

# Modify mojette threads threshold
# rozofs.set_mojette_threads_threshold(32*1024)

# Dual STORCLI
# rozofs.dual_storcli()

# Disable POSIX lock
#rozofs.no_posix_lock()

# Disable BSD lock
#rozofs.no_bsd_lock()


# Client fast reconnect
#rozofs.set_client_fast_reconnect()

#-------------- NB devices
devices    = 3
mapper     = 3
redundancy = 2

# Nb cluster per volume
nbclusters = 3

# default is to have one mount point per site
clients_nb = rozofs.site_number

# Define Layout
setLayout(1)

# Define number of Host 
vol = setVolumeHosts(8)
addExport(vol,1)
addExport(vol,0)


# Set host 1 faulty
#h1 = host_class.get_host(1)
#if h1 == None:
#  print "Can find host 1"
#else:
#  h1.set_admin_off()  
  
