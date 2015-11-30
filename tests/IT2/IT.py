#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os.path
import subprocess
import time
import re
import shlex
from adaptative_tbl import *
import syslog
from optparse import OptionParser

fileSize=int(4)
loop=int(32)
process=int(8)
EXPORT_SID_NB=int(8)
STORCLI_SID_NB=int(8)
nbGruyere=int(256)
stopOnFailure=True
fuseTrace=False
DEFAULT_MNT="mnt1_1_g0"
ALL_MNT="mnt1_1_g0,mnt2_1_g0,mnt3_1_g0,mnt4_1_g0"
mnts=DEFAULT_MNT
mnt=""
DEFAULT_RETRIES=int(20)
tst_file="tst_file"
device_number=""
mapper_modulo=""
mapper_redundancy=""

ifnumber=int(0)
instance=None
site=None
eid=None
vid=None
mnt=None
inverse=None
forward=None
safe=None
nb_failures=None
sids=[]
hosts=[]

#___________________________________________________
def clean_cache(val=1): os.system("echo %s > /proc/sys/vm/drop_caches"%val)
#___________________________________________________

#___________________________________________________
def clean_rebuild_dir():
#___________________________________________________
  #os.system("mkdir -p /tmp/rebuild/; mv -f /var/run/storage_rebuild/* /tmp/rebuild/");
  os.system("mkdir -p /tmp/rebuild/");
 
#___________________________________________________
def my_duration (val):
#___________________________________________________

  hour=val/3600  
  min=val%3600  
  sec=min%60
  min=min/60
  return "%2d:%2.2d:%2.2d"%(hour,min,sec)

#___________________________________________________
def reset_counters():
# Use debug interface to reset profilers and some counters
#___________________________________________________
  return

#___________________________________________________
def get_device_numbers(hid,cid):
# Use debug interface to get the number of sid from exportd
#___________________________________________________
  device_number=1
  mapper_modulo=1
  mapper_redundancy=1 

  storio_name="storio:0"
  
  string="./build/src/rozodiag/rozodiag -i localhost%s -T storaged -c storio"%(hid)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:
    if "mode" in line:
      if "multiple" in line:
        storio_name="storio:%s"%(cid)
      break; 
     
  string="./build/src/rozodiag/rozodiag -i localhost%s -T %s -c device 1> /dev/null"%(hid,storio_name)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  for line in cmd.stdout:
    if "device_number" in line:
      device_number=line.split()[2]
    if "mapper_modulo" in line:
      mapper_modulo=line.split()[2]
    if "mapper_redundancy" in line:
      mapper_redundancy=line.split()[2]
      
  return device_number,mapper_modulo,mapper_redundancy   
#___________________________________________________
def get_if_nb():

  string="./setup.py display"     
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  for line in cmd.stdout:
    if "Listen" in line:
      words=line.split()
      for idx in range(0,len(words)):
        if words[idx] == "Listen": return words[idx+2]
  return 0	

#___________________________________________________
def get_sid_nb():
# Use debug interface to get the number of sid from exportd
#___________________________________________________

  string="./build/src/rozodiag/rozodiag -T mount:%s:1 -c storaged_status"%(instance)       
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  storcli_sid=int(0)
  for line in cmd.stdout:
    if "UP" in line or "DOWN" in line:
      storcli_sid=storcli_sid+1
          
  string="./build/src/rozodiag/rozodiag -T export -c vfstat_stor"
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  export_sid=int(0)
  for line in cmd.stdout:
    if len(line.split()) == 0: continue
    if line.split()[0] != vid: continue;
    if "UP" in line or "DOWN" in line:export_sid=export_sid+1

  return export_sid,storcli_sid    
#___________________________________________________
def reset_storcli_counter():
# Use debug interface to get the number of sid from exportd
#___________________________________________________

  string="./build/src/rozodiag/rozodiag -T mount:%s:1 -c counter reset"%(instance)       
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#___________________________________________________
def check_storcli_crc():
# Use debug interface to get the number of sid from exportd
#___________________________________________________

  string="./build/src/rozodiag/rozodiag -T mount:%s:1 -c profiler"%(instance)       
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  for line in cmd.stdout:
    if "read_blk_crc" in line:
      return True   
  return False    
   
#___________________________________________________
def export_count_sid_up ():
# Use debug interface to count the number of sid up 
# seen from the export. 
#___________________________________________________
  global vid
  
  string="./build/src/rozodiag/rozodiag -T export -c vfstat_stor"
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  match=int(0)
  for line in cmd.stdout:
    if len(line.split()) == 0:
      continue
    if line.split()[0] != vid:
      continue
    if "UP" in line:
      match=match+1

  return match
 
#___________________________________________________
def storcli_count_sid_available ():
# Use debug interface to count the number of sid 
# available seen from the storcli. 
#___________________________________________________

  string="./build/src/rozodiag/rozodiag -T mount:%s:1 -c storaged_status"%(instance)       
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  # Looking for state=UP and selectable=YES
  match=int(0)
  for line in cmd.stdout:
    words=line.split('|')
    if len(words) >= 11:
      if 'YES' in words[6] and 'UP' in words[4]:
        match=match+1
    
  return match  

#___________________________________________________
def loop_wait_until (success,retries,function):
# Loop until <function> returns <success> for a maximum 
# of <retries> attempt (one attempt per second)
#___________________________________________________
  up=int(0)

  while int(up) != int(success):

    retries=retries-1
    if retries == 0:
      print "Maximum retries reached. %s is %s\n"%(function,up)      
      return False
      
    sys.stdout.write(".")
    sys.stdout.flush()     
     
    up=getattr(sys.modules[__name__],function)()
    time.sleep(1)
   
  time.sleep(1)    
  return True  
#___________________________________________________
def loop_wait_until_less (success,retries,function):
# Loop until <function> returns <success> for a maximum 
# of <retries> attempt (one attempt per second)
#___________________________________________________
  up=int(0)

  while int(up) >= int(success):

    retries=retries-1
    if retries == 0:
      print "Maximum retries reached. %s is %s\n"%(function,up)      
      return False
      
    sys.stdout.write(".")
    sys.stdout.flush()     
     
    up=getattr(sys.modules[__name__],function)()
    time.sleep(1)
   
  time.sleep(1)    
  return True    
#___________________________________________________
def start_all_sid () :
# Wait for all sid up seen by storcli as well as export
#___________________________________________________
  for sid in range(STORCLI_SID_NB):
    hid=sid+(site*STORCLI_SID_NB)
    os.system("./setup.py storage %s start"%(hid+1))    
    

      
#___________________________________________________
def wait_until_all_sid_up (retries=DEFAULT_RETRIES) :
# Wait for all sid up seen by storcli as well as export
#___________________________________________________
  if loop_wait_until(STORCLI_SID_NB,retries,'storcli_count_sid_available') == False: 
    print "storcli_count_sid_available %s failed"%(STORCLI_SID_NB)
    return False
  if loop_wait_until(EXPORT_SID_NB,retries,'export_count_sid_up') == False:
    print "export_count_sid_up %s failed"%(EXPORT_SID_NB)
    return False
  return True  
  
    
#___________________________________________________
def wait_until_one_sid_down (retries=DEFAULT_RETRIES) :
# Wait until one sid down seen by storcli 
#___________________________________________________

  if loop_wait_until_less(STORCLI_SID_NB,retries,'storcli_count_sid_available') == False:
    return False
  return True   
#___________________________________________________
def wait_until_x_sid_down (x,retries=DEFAULT_RETRIES) :
# Wait until one sid down seen by storcli 
#___________________________________________________

  if loop_wait_until_less(int(STORCLI_SID_NB)-int(x),retries,'storcli_count_sid_available') == False:
    return False
  return True   
#___________________________________________________
def storageStart (hid,count=int(1)) :

  sys.stdout.write("\r                                   ")
  sys.stdout.write("\rStorage start ")

  for idx in range(int(count)): 
    sys.stdout.write("%s "%(int(hid)+idx)) 
    sys.stdout.flush()
    os.system("./setup.py storage %s start"%(int(hid)+idx))
        
#___________________________________________________
def storageStartAndWait (hid,count=int(1)) :

  storageStart(hid,count)
  time.sleep(1)
  if wait_until_all_sid_up() == True:
    return 0
        
  return 1 
#___________________________________________________
def storageStop (hid,count=int(1)) :

  sys.stdout.write("\r                                   ")
  sys.stdout.write("\rStorage stop ")

  for idx in range(int(count)): 
    sys.stdout.write("%s "%(int(hid)+idx)) 
    sys.stdout.flush()
    os.system("./setup.py storage %s stop"%(int(hid)+idx))
  
#___________________________________________________
def storageStopAndWait (hid,count=int(1)) :

  storageStop(hid,count)
  time.sleep(1)
  wait_until_x_sid_down(count)   
    
#___________________________________________________
def storageFailed (test) :
# Run test names <test> implemented in function <test>()
# under the circumstance that a storage is stopped
#___________________________________________________        
  global hosts     
       
  # Wait all sid up before starting the test     
  if wait_until_all_sid_up() == False:
    return 1
    
  # Loop on hosts
  for hid in hosts:  
   
    # Process hosts in a bunch of allowed failures	   
    if int(nb_failures) != int(1):
      if int(hid)%int(nb_failures) != int(1):
        continue
      	    
    # Reset a bunch of storages	    
    storageStopAndWait(hid,nb_failures)
    reset_counters()
    
    # Run the test
    try:
      # Resolve and call <test> function
      ret = getattr(sys.modules[__name__],test)()         
    except:
      print "Error on %s"%(test)
      ret = 1
      
    # Restart every storages  
    storageStartAndWait(hid,nb_failures)  

    if ret != 0:
      return 1      

      
  return 0

#___________________________________________________
def snipper_storcli ():
# sub process that periodicaly resets the storcli(s)
#___________________________________________________
  
  while True:

      sys.stdout.write("\r                                 ")
      sys.stdout.flush()  
      sys.stdout.write("\rStorcli reset")
      sys.stdout.flush()

      p = subprocess.Popen(["ps","-ef"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      for proc in p.stdout:
        if not "storcli -i" in proc:
          continue
        if "rozolauncher" in proc:
          continue
        if not "%s"%(mnt) in proc:
          continue  
	
        pid=proc.split()[1]
        os.system("kill -9 %s"%(pid))
	    

      for i in range(9):
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(1)

#___________________________________________________
def storcliReset (test):
# Run test names <test> implemented in function <test>()
# under the circumstance where storcli is periodicaly reset
#___________________________________________________

  global loop

  time.sleep(3)
 
  # Start process that reset the storages
  string="./IT2/IT.py --snipper storcli --mount %s"%(mnt)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stderr=subprocess.PIPE)

  saveloop=loop
  loop=loop*2
  
  try:
    # Resolve and call <test> function
    ret = getattr(sys.modules[__name__],test)()         
  except:
    ret = 1
    
  loop=saveloop

  # kill the storcli snipper process
  cmd.kill()

  if ret != 0:
      return 1
  return 0

#___________________________________________________
def snipper_if ():
# sub process that periodicaly resets the storio(s)
#___________________________________________________
  global ifnumber
  
  while True:
      
    for itf in range(0,int(ifnumber)):

      for hid in hosts:     
	  
	sys.stdout.write("\r                                 ")
	sys.stdout.flush()
	sys.stdout.write("\rhost %s if %s down "%(hid,itf))
        sys.stdout.flush()
	
	os.system("./setup.py storage %s ifdown %s"%(hid,itf))
	time.sleep(1)
	
          
	sys.stdout.write("\r                                 ")
	sys.stdout.flush()
	sys.stdout.write("\rhost %s if %s up   "%(hid,itf))
        sys.stdout.flush()
	
	os.system("./setup.py storage %s ifup %s"%(hid,itf))
	time.sleep(0.2)  
#___________________________________________________
def ifUpDown (test):
# Run test names <test> implemented in function <test>()
# under the circumstance that the interfaces goes up and down
#___________________________________________________
  global loop
  
 
  # Start process that reset the storages
  string="./IT2/IT.py --snipper if --mount %s"%(instance)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stderr=subprocess.PIPE)

  saveloop=loop
  loop=loop*8
  
  try:
    # Resolve and call <test> function
    ret = getattr(sys.modules[__name__],test)() 
  except:
    ret = 1

  loop=saveloop

  # kill the storio snipper process
  cmd.kill()

  if ret != 0:
      return 1
  return 0
  
  
#___________________________________________________
def snipper_storage ():
# sub process that periodicaly resets the storio(s)
#___________________________________________________
    
  while True:
    for hid in hosts:      

      # Process hosts in a bunch of allowed failures	   
      if int(nb_failures)!= int(1):
	if int(hid)%int(nb_failures) != int(1): continue

      # Wait all sid up before starting the test     
      if wait_until_all_sid_up() == False: return 1
      
      time.sleep(0.25)
          
      sys.stdout.write("\r                                 ")
      sys.stdout.flush()
      sys.stdout.write("\rStorage reset ")
      cmd=""
      for idx in range(int(nb_failures)):
        val=int(hid)+int(idx)
        sys.stdout.write("%s "%(val))
	cmd+="./setup.py storage %s reset;"%(val)
	
      sys.stdout.flush()
      os.system(cmd)



  
#___________________________________________________
def storageReset (test):
# Run test names <test> implemented in function <test>()
# under the circumstance that storio(s) are periodicaly reset
#___________________________________________________
  global loop
  
 
  # Start process that reset the storages
  string="./IT2/IT.py --snipper storage --mount %s"%(instance)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stderr=subprocess.PIPE)

  saveloop=loop
  loop=loop*8
  
  try:
    # Resolve and call <test> function
    ret = getattr(sys.modules[__name__],test)() 
  except:
    ret = 1
    
  loop=saveloop

  # kill the storio snipper process
  cmd.kill()

  if ret != 0:
      return 1
  return 0

#___________________________________________________
def snipper (target):
# A snipper command has been received for a given target. 
# Resolve function snipper_<target> and call it.
#___________________________________________________

  func='snipper_%s'%(target)
  try:
    ret = getattr(sys.modules[__name__],func)()         
  except:
    print "Failed snipper %s"%(func)
    ret = 1
  return ret  

#___________________________________________________
def wr_rd_total ():
#___________________________________________________
  ret=os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -total -mount %s"%(process,loop,fileSize,tst_file,mnt))
  return ret  

#___________________________________________________
def wr_rd_partial ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -partial -mount %s"%(process,loop,fileSize,tst_file,mnt))

#___________________________________________________
def wr_rd_random ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -random -mount %s"%(process,loop,fileSize,tst_file,mnt))

#___________________________________________________
def wr_rd_total_close ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -total -file %s -closeAfter -mount %s"%(process,loop,fileSize,tst_file,mnt))

#___________________________________________________
def wr_rd_partial_close ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -partial -file %s -closeAfter -mount %s"%(process,loop,fileSize,tst_file,mnt))

#___________________________________________________
def wr_rd_random_close ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -random -file %s -closeAfter -mount %s"%(process,loop,fileSize,tst_file,mnt))

#___________________________________________________
def wr_close_rd_total ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -total -closeBetween -mount %s"%(process,loop,fileSize,tst_file,mnt))

#___________________________________________________
def wr_close_rd_partial ():
#___________________________________________________
  ret=os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -partial -closeBetween -mount %s"%(process,loop,fileSize,tst_file,mnt))
  return ret 

#___________________________________________________
def wr_close_rd_random ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -random -closeBetween -mount %s"%(process,loop,fileSize,tst_file,mnt))

#___________________________________________________
def wr_close_rd_total_close ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -total -closeBetween -closeAfter -mount %s"%(process,loop,fileSize,tst_file,mnt))

#___________________________________________________
def wr_close_rd_partial_close ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -partial -closeBetween -closeAfter -mount %s"%(process,loop,fileSize,tst_file,mnt))

#___________________________________________________
def wr_close_rd_random_close ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -random -closeBetween -closeAfter -mount %s"%(process,loop,fileSize,tst_file,mnt))

#___________________________________________________
def rw2 ():
#___________________________________________________
  return os.system("./IT2/rw2.exe -loop %s -file %s/%s"%(loop,mnt,tst_file))
#___________________________________________________
def write_parallel ():
#___________________________________________________
  return os.system("./IT2/write_parallel.exe -file %s/%s"%(mnt,tst_file))

#___________________________________________________
def prepare_file_to_read(filename,mega):
#___________________________________________________

  if not os.path.exists(filename):
    os.system("dd if=/dev/zero of=%s bs=1M count=%s 1> /dev/null"%(filename,mega))    

#___________________________________________________
def read_parallel ():
#___________________________________________________

  zefile='%s/%s'%(mnt,tst_file)
  prepare_file_to_read(zefile,fileSize) 
  ret=os.system("./IT2/read_parallel.exe -process %s -loop %s -file %s"%(process,loop,zefile)) 
  return ret 
#___________________________________________________
def crc32_reread():
  ret = os.system("./IT2/test_crc32.exe -action REREAD -mount %s -file crc32"%(mnt))
  if ret != 0:
    print "Reread error"
  return ret    
#___________________________________________________
def crc32():
#___________________________________________________

  # Clear error counter
  reset_storcli_counter()
  # Check CRC errors 
  if check_storcli_crc():
    print "CRC errors after counter reset"
    return 1 
    
  # Create CRC32 errors  
  os.system("./IT2/test_crc32.exe -action CREATE -mount %s -file crc32"%(mnt))
  os.system("./IT2/test_crc32.exe -action CORRUPT -mount %s -file crc32"%(mnt))
  ret = crc32_reread()
  if ret != 0: return ret
  
  # Check CRC errors 
  if check_storcli_crc() == False: 
    print "No CRC errors after test"  
    return 0
  
  return storageFailed('crc32_reread')
 
#___________________________________________________
def xattr():
#___________________________________________________
  return os.system("./IT2/test_xattr.exe -process %d -loop %d -mount %s"%(process,loop,mnt))

#___________________________________________________
def link():
#___________________________________________________
  return os.system("./IT2/test_link.exe -process %d -loop %d -mount %s"%(process,loop,mnt))

#___________________________________________________
def symlink():
#___________________________________________________
  return os.system("./IT2/test_symlink.exe -process %d -loop %d -mount %s"%(process,loop,mnt))

#___________________________________________________
def readdir():
#___________________________________________________ 
  return os.system("./IT2/test_readdir.exe -process %d -loop %d -mount %s"%(process,loop,mnt))

#___________________________________________________
def rename():
#___________________________________________________
  ret=os.system("./IT2/test_rename.exe -process %d -loop %d -mount %s"%(process,loop,mnt))
  return ret 

#___________________________________________________
def chmod():
#___________________________________________________
  return os.system("./IT2/test_chmod.exe -process %d -loop %d -mount %s"%(process,loop,mnt))

#___________________________________________________
def truncate():
#___________________________________________________
  return os.system("./IT2/test_trunc.exe -process %d -loop %d -mount %s"%(process,loop,mnt))

#___________________________________________________
def lock_posix_passing():
#___________________________________________________ 
  zefile='%s/%s'%(mnt,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_file_lock.exe -process %d -loop %d -file %s -nonBlocking"%(process,loop,zefile))  

#___________________________________________________
def lock_posix_blocking():
#___________________________________________________
  zefile='%s/%s'%(mnt,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  

  ret=os.system("./IT2/test_file_lock.exe -process %d -loop %d -file %s"%(process,loop,zefile))
  return ret 

#___________________________________________________
def lock_bsd_passing():
#___________________________________________________  
  zefile='%s/%s'%(mnt,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_file_lock.exe -process %d -loop %d -file %s -nonBlocking -bsd"%(process,loop,zefile))


#___________________________________________________
def quiet(val=10):
#___________________________________________________

  while True:
    time.sleep(val)


#___________________________________________________
def lock_bsd_blocking():
#___________________________________________________
  zefile='%s/%s'%(mnt,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_file_lock.exe -process %d -loop %d -file %s -nonBlocking"%(process,loop,zefile))  

#___________________________________________________
def gruyere_one_reread():
# reread files create by test_rebuild utility to check
# their content
#___________________________________________________ 
  clean_cache()
  syslog.syslog("re-read %d files"%(int(nbGruyere)))
  res=os.system("./IT2/test_rebuild.exe -action check -nbfiles %d -mount %s"%(int(nbGruyere),mnt))
  syslog.syslog("re-read result %s"%(res))
  return res
  
#___________________________________________________
def gruyere_reread():
# reread files create by test_rebuild with every storage
# possible fault to check every projection combination
#___________________________________________________

  ret = gruyere_one_reread()
  if ret != 0:
    return ret
    
  ret = storageFailed('gruyere_one_reread')
  time.sleep(3)
  return ret

#___________________________________________________
def gruyere_write():
# Use test_rebuild utility to create a bunch of files
#___________________________________________________ 
  return os.system("rm -f %s/rebuild/*; ./IT2/test_rebuild.exe -action create -nbfiles %d -mount %s"%(mnt,int(nbGruyere),mnt))  
#___________________________________________________
def gruyere():
# call gruyere_write that create a bunch of files while
# block per block while storages are reset. This makes
# files with block dispersed on every storage. 
#___________________________________________________

  ret = storageReset('gruyere_write')
  if ret != 0:  
    return ret
  if rebuildCheck == True: 
    return gruyere_reread()
  return 0
#___________________________________________________
def rebuild_one_dev() :
# test rebuilding device per device
#___________________________________________________
  global sids

  ret=1 
  for s in sids:
    
    hid=s.split('-')[0]
    cid=s.split('-')[1]
    sid=s.split('-')[2]
        
    device_number,mapper_modulo,mapper_redundancy = get_device_numbers(hid,cid)
    
    dev=int(hid)%int(mapper_modulo)
    clean_rebuild_dir()    
    string="./setup.py sid %s %s rebuild -fg -d %s -o one_cid%s_sid%s_dev%s 1> /dev/null"%(cid,sid,dev,cid,sid,dev)
    os.system("./setup.py sid %s %s device-delete %s"%(cid,sid,dev))
    os.system("./setup.py sid %s %s device-create %s"%(cid,sid,dev))
    ret = os.system(string)
    if ret != 0:
      return ret
      
    if int(mapper_modulo) > 1:
      dev=(dev+1)%int(mapper_modulo)
      os.system("./setup.py sid %s %s device-delete %s"%(cid,sid,dev))
      os.system("./setup.py sid %s %s device-create %s"%(cid,sid,dev))
      ret = os.system("./setup.py sid %s %s rebuild -fg -d %s -o one_cid%s_sid%s_dev%s 1> /dev/null"%(cid,sid,dev,cid,sid,dev))
      if ret != 0:
	return ret
	
    if rebuildCheck == True:      
      ret = gruyere_one_reread()  
      if ret != 0:
        return ret 

  if rebuildCheck == True:      
    ret = gruyere_reread()          
    return ret
  return 0
#___________________________________________________
def relocate_one_dev() :
# test rebuilding device per device
#___________________________________________________


  ret=1 
  for s in sids:
    
    hid=s.split('-')[0]
    cid=s.split('-')[1]
    sid=s.split('-')[2]

    # Get storio mode: single or multuple
    string="./build/src/rozodiag/rozodiag -i localhost%s -T storaged -c storio_nb"%(hid)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    mode = "unknown"
    for line in cmd.stdout:
      if "mode" in line:
        words=line.split()
	mode=words[2]
	break;     

    # Check wether self healing is configured
    string="./build/src/rozodiag/rozodiag -i localhost%s -T storio:%s -c device "%(hid,cid)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    for line in cmd.stdout:
      if "self-healing" in line:
        words=line.split()
	selfHealing=words[2]
	break; 

    clean_rebuild_dir()
     
    # No self healing configured. Ask for a rebuild with relocation	      
    if selfHealing == "No":
      syslog.syslog("No self healing => call relocate")	      
      ret = os.system("./setup.py sid %s %s rebuild -fg -d 0 -o reloc_cid%s_sid%s_dev0 1> /dev/null"%(cid,sid,cid,sid))
      if ret != 0:
	return ret
	
    # Self healing is configured. Remove device and wait for automatic relocation	
    else:
    
      ret = os.system("./setup.py sid %s %s device-delete 0"%(cid,sid))
      if ret != 0:
	return ret
	      	
      count=int(50)	
      status="INIT"
      
      while count != int(0):

	if "OOS" in status:
	  syslog.syslog("count %d device is %s"%(count,status))
	  break        

        count=count-1
        time.sleep(10)
	
        # Check The status of the device
        if mode == "multiple": string="./build/src/rozodiag/rozodiag -i localhost%s -T storio:%s -c device 1> /dev/null"%(hid,cid)
	else                 : string="./build/src/rozodiag/rozodiag -i localhost%s -T storio:0 -c device 1> /dev/null"%(hid)
	parsed = shlex.split(string)
	cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	curcid=0
	cursid=0
	for line in cmd.stdout:
	
          # Read cid sid
	  if "cid =" in line and "sid =" in line:
	    words=line.split()
	    curcid=int(words[2])
	    cursid=int(words[5])
	    continue
	      
          if int(curcid) != int(cid) or int(cursid) != int(sid): continue 
          words=line.split('|')
	  try:
	    if int(words[0]) != int(0):
	      continue
	    status=words[1]
	    break
	  except:
	    pass    
	    
      if count == int(0):
        print "Relocate failed host %s cluster %s sid %s device 0 status %s"%(hid,cid,sid,status)
	return 1
	
    if rebuildCheck == True:		      
      ret = gruyere_one_reread()  
      if ret != 0:
	return ret 
      
    if selfHealing != "No":
      ret = os.system("./setup.py sid %s %s device-create 0"%(cid,sid))
      ret = os.system("./setup.py sid %s %s rebuild -fg -d 0 -C -o clear_cid%s_sid%s_dev0 1> /dev/null"%(cid,sid,cid,sid))
      
      
  if rebuildCheck == True:      
    ret = gruyere_reread()          
    return ret         
  return 0

#___________________________________________________
def rebuild_all_dev() :
# test re-building all devices of a sid
#___________________________________________________

  ret=1 
  for s in sids:
    
    hid=s.split('-')[0]
    cid=s.split('-')[1]
    sid=s.split('-')[2]

    clean_rebuild_dir()

    os.system("./setup.py sid %s %s device-delete all 1> /dev/null"%(cid,sid))
    os.system("./setup.py sid %s %s device-create all 1> /dev/null"%(cid,sid))
    ret = os.system("./setup.py sid %s %s rebuild -fg -o all_cid%s_sid%s 1> /dev/null"%(cid,sid,cid,sid))
    if ret != 0:
      return ret

    if rebuildCheck == True:	
      ret = gruyere_one_reread()  
      if ret != 0:
	return ret    

  if rebuildCheck == True:      
    ret = gruyere_reread()          
    return ret
  return 0  

#___________________________________________________
def rebuild_one_node() :
# test re-building a whole storage
#___________________________________________________
  global hosts
  global sids
    
  ret=1 
  for hid in hosts:
 
    for s in sids:

      zehid=s.split('-')[0]
      if int(zehid) != int(hid): continue
      
      cid=s.split('-')[1]
      sid=s.split('-')[2]
      
      os.system("./setup.py sid %s %s device-delete all 1> /dev/null"%(cid,sid))
      os.system("./setup.py sid %s %s device-create all 1> /dev/null"%(cid,sid))      

    clean_rebuild_dir()
    
    string="./setup.py storage %s rebuild -fg -o node_%s 1> /dev/null"%(hid,hid)
    ret = os.system(string)
    if ret != 0:
      return ret

    if rebuildCheck == True:	
      ret = gruyere_one_reread()  
      if ret != 0:
	return ret    

  if rebuildCheck == True:      
    ret = gruyere_reread()          
    return ret
  return 0  
#___________________________________________________
def delete_rebuild() :
# test re-building a whole storage
#___________________________________________________
  os.system("rm -rf %s/rebuild  1> /dev/null"%(mnt))
  return 0
#___________________________________________________
def rebuild_fid() :
# test rebuilding per FID
#___________________________________________________
  skip=0

  for f in range(int(nbGruyere)/10):
  
    skip=skip+1
    if skip == 4:
      skip=0  

    # Get the split of file on storages      
#    string="./setup.py cou %s/rebuild/%d"%(mnt,f+1)
    string="attr -g rozofs %s/rebuild/%d"%(mnt,f+1)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # loop on the bins file constituting this file, and ask
    # the storages for a rebuild of the file
    bins_list = []
    fid=""
    cid=0
    storages=""
    for line in cmd.stdout:
	  
      if "FID" in line:
        words=line.split();
	if len(words) >= 2:
          fid=words[2]
	  continue
	  
      if "CLUSTER" in line:
        words=line.split();
	if len(words) >= 2:
          cid=words[2]
	  continue
	  
      if "STORAGE" in line:
        words=line.split();
	if len(words) >= 2:
          storages=words[2]
	  continue	  	  	    
      
    # loop on the bins file constituting this file, and ask
    # the storages for a rebuild of the file
    line_nb=0

    for sid in storages.split('-'):
    
      sid=int(sid)
      line_nb=line_nb+1
      if skip >= line_nb:
	  continue;  

      clean_rebuild_dir()
	    	
      string="./setup.py sid %s %s rebuild -fg -f %s -o fid%s_cid%s_sid%s 1> /dev/null"%(cid,sid,fid,fid,cid,sid)
      parsed = shlex.split(string)
      cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      cmd.wait()

      if cmd.returncode != 0:
        print "%s failed"%(string)
	return 1 	       

  if rebuildCheck == True:      
    ret = gruyere_reread()          
    return ret
  return 0   

#___________________________________________________
def append_circumstance_test_list(list,input_list,circumstance):
# Add to <list> the list <input_list> prefixed with 
# <circumstance> that should be a valid circumstance test list.
# function <circumstance>() should exist to implement this
# particuler test circumstance.
#___________________________________________________

   for tst in input_list:
     list.append("%s/%s"%(circumstance,tst)) 

#___________________________________________________
def do_compile_program(program): 
# compile program if program.c is younger
#___________________________________________________

  if not os.path.exists("%s.exe"%(program)) or os.stat("%s.exe"%(program)).st_mtime < os.stat("%s.c"%(program)).st_mtime:
    os.system("gcc -g %s.c -lpthread -o %s.exe"%(program,program))

#___________________________________________________
def do_compile_programs(): 
# compile all program if program.c is younger
#___________________________________________________
  dirs=os.listdir("%s/IT2"%(os.getcwd()))
  for file in dirs:
    if ".c" not in file:
      continue
    words=file.split('.')
    prg=words[0]   
    do_compile_program("IT2/%s"%(prg))

#___________________________________________________
def do_run_list(list):
# run a list of test
#___________________________________________________
  global tst_file
  
  tst_num=int(0)
  failed=int(0)
  success=int(0)
  
  dis = adaptative_tbl(4,"TEST RESULTS")
  dis.new_center_line()
  dis.set_column(1,'#')
  dis.set_column(2,'Name')
  dis.set_column(3,'Result')
  dis.set_column(4,'Duration')
  dis.end_separator()  

  time_start=time.time()
  
  total_tst=len(list)    
  for tst in list:

    tst_num=tst_num+1
    
    syslog.syslog("%10s ........ %s"%("START TEST",tst))
    
    sys.stdout.write( "\r___%4d/%d : %-40s \n"%(tst_num,total_tst,tst))

    dis.new_line()  
    dis.set_column(1,'%s'%(tst_num))
    dis.set_column(2,tst)

    
    # Split optional circumstance and test name
    split=tst.split('/') 
    
    time_before=time.time()
    reset_counters()   
    tst_file="tst_file" 
    
    if len(split) > 1:
    
      tst_file="%s.%s"%(split[1],split[0])
    
      # There is a test circumstance. resolve and call the circumstance  
      # function giving it the test name
      try:
        ret = getattr(sys.modules[__name__],split[0])(split[1])          
      except:
        ret = 1

    else:

      tst_file=split[0]


      
      # No test circumstance. Resolve and call the test function
      try:
        ret = getattr(sys.modules[__name__],split[0])()
      except:
        ret = 1
	
    delay=time.time()-time_before;	
    dis.set_column(4,'%s'%(my_duration(delay)))
    
    if ret == 0:
      syslog.syslog("%10s %8s %s"%("SUCCESS",my_duration(delay),tst))    
      dis.set_column(3,'OK')
      success=success+1
    else:
      syslog.syslog("%10s %8s %s"%("FAILURE",my_duration(delay),tst))        
      dis.set_column(3,'FAILED')
      failed=failed+1
      
    if failed != 0 and stopOnFailure == True:
        break
         
    
  dis.end_separator()   
  dis.new_line()  
  dis.set_column(1,'%s'%(success+failed))
  dis.set_column(2,mnt)
  if failed == 0:
    dis.set_column(3,'OK')
  else:
    dis.set_column(3,'%d FAILED'%(failed))
    
  delay=time.time()-time_start    
  dis.set_column(4,'%s'%(my_duration(delay)))
  
  print ""
  dis.display()        
  
     
#___________________________________________________
def do_list():
# Display the list of all tests
#___________________________________________________

  num=int(0)
  dis = adaptative_tbl(4,"TEST LIST")
  dis.new_center_line()  
  dis.set_column(1,'Number')
  dis.set_column(2,'Test name')
  dis.set_column(3,'Test group')
  
  dis.end_separator()  
  for tst in TST_BASIC:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,tst)
    dis.set_column(3,'basic')  
    
  dis.end_separator()         
  for tst in TST_REBUILD:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,tst)
    dis.set_column(3,'rebuild') 
    
  dis.end_separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,tst)
    dis.set_column(3,'rw') 

  dis.end_separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,"%s/%s"%('storageFailed',tst))
    dis.set_column(3,'storageFailed') 

  dis.end_separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,"%s/%s"%('storageReset',tst))
    dis.set_column(3,'storageReset') 

  dis.end_separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,"%s/%s"%('ifUpDown',tst))
    dis.set_column(3,'ifUpDown') 
    
  dis.end_separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,"%s/%s"%('storcliReset',tst))
    dis.set_column(3,'storcliReset')  

  dis.display()    
#____________________________________
def resolve_sid(cid,sid):

  string="%s/setup.py sid %s %s info"%(os.getcwd(),cid,sid)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:
    words = line.split()
    if len(words)<3: continue
    if words[0] == "site0": site0=words[2]
    if words[0] == "path0" : path0=words[2]
          
  try:int(site0)
  except:
    print "No such cid/sid %s/%s"%(cid,sid)
    return -1,"" 
  return site0,path0
       
#____________________________________
def resolve_mnt(inst):
  global site
  global eid
  global vid
  global mnt
  global inverse
  global forward
  global safe
  global instance
  global nb_failures
  global sids
  global hosts
    
  vid="A"
  pid=None  
  instance = inst
        
  string="%s/setup.py mount %s info"%(os.getcwd(),instance)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:
    words = line.split()
    if len(words)<3: continue
    if words[0] == "site": site=words[2]
    if words[0] == "eid" : eid=words[2]
    if words[0] == "vid" : vid=words[2]
    if words[0] == "failures": nb_failures=int(words[2])
    if words[0] == "hosts": hosts=line.split("=")[1].split()
    if words[0] == "sids": sids=line.split("=")[1].split()
    if words[0] == "path": mnt=words[2]
    if words[0] == "pid": pid=words[2]
    if words[0] == "layout": 
      inverse=words[2]
      forward=words[3]
      safe=words[4]
          
  try:int(vid)
  except:
    print "No such RozoFS mount instance %s"%(instance)
    exit(1)    
    
  if pid == None:
    print "RozoFS instance %s is not running"%(instance)
    exit(1)      
    
#___________________________________________________
def usage():
#___________________________________________________

  print "\n./IT2/IT.py -l"
  print "  Display the whole list of tests."
  print "\n./IT2/IT.py [options] [extra] <test name/group> [<test name/group>...]"      
  print "  Runs a test list."
  print "    options:"
  print "      [--mount <mount1,mount2,..>]  A comma separated list of mount point instances. (default 1)"   
  print "      [--speed]          The run 4 times faster tests."
  print "      [--fast]           The run 2 times faster tests."
  print "      [--long]           The run 2 times longer tests."
  print "      [--repeat <nb>]    The number of times the test list must be repeated."   
  print "      [--cont]           To continue tests on failure." 
  print "      [--fusetrace]      To enable fuse trace on test. When set, --stop is automaticaly set."
  print "    extra:"
  print "      [--process <nb>]   The number of processes that will run the test in paralell. (default %d)"%(process)
  print "      [--count <nb>]     The number of loop that each process will do. (default %s)"%(loop) 
  print "      [--fileSize <nb>]  The size in MB of the file for the test. (default %d)"%(fileSize)   
  print "      [--rebuildCheck]   To check strictly after each rebuild that the files are secured."
  print "    Test group and names can be displayed thanks to ./IT2/IT.py -l"
  print "       - all              designate all the tests."
  print "       - rw               designate the read/write test list."
  print "       - storageFailed    designate the read/write test list run when a storage is failed."
  print "       - storageReset     designate the read/write test list run while a storage is reset."
  print "       - storcliReset     designate the read/write test list run while the storcli is reset."
  print "       - basic            designate the non read/write test list."
  print "       - rebuild          designate the rebuild test list."
  exit(0)



#___________________________________________________
# MAIN
#___________________________________________________                  
parser = OptionParser()
parser.add_option("-v","--verbose", action="store_true",dest="verbose", default=False, help="To set the verbose mode")
parser.add_option("-p","--process", action="store",type="string", dest="process", help="The number of processes that will run the test in paralell")
parser.add_option("-c","--count", action="store", type="string", dest="count", help="The number of loop that each process will do.")
parser.add_option("-f","--fileSize", action="store", type="string", dest="fileSize", help="The size in MB of the file for the test.")
parser.add_option("-l","--list",action="store_true",dest="list", default=False, help="To display the list of test")
parser.add_option("-k","--snipper",action="store",type="string",dest="snipper", help="To start a storage/storcli snipper.")
parser.add_option("-s","--cont", action="store_true",dest="cont", default=False, help="To continue on failure.")
parser.add_option("-t","--fusetrace", action="store_true",dest="fusetrace", default=False, help="To enable fuse trace on test.")
parser.add_option("-F","--fast", action="store_true",dest="fast", default=False, help="To run 2 times faster tests.")
parser.add_option("-S","--speed", action="store_true",dest="speed", default=False, help="To run 4 times faster tests.")
parser.add_option("-L","--long", action="store_true",dest="long", default=False, help="To run 2 times longer tests.")
parser.add_option("-r","--repeat", action="store", type="string", dest="repeat", help="A repetition count.")
parser.add_option("-m","--mount", action="store", type="string", dest="mount", help="A comma separated list of mount points to test on.")
parser.add_option("-R","--rebuildCheck", action="store_true", dest="rebuildCheck", default=False, help="To request for stron rebuild check after each rebuild.")

# Read/write test list
TST_RW=['read_parallel','write_parallel','rw2','wr_rd_total','wr_rd_partial','wr_rd_random','wr_rd_total_close','wr_rd_partial_close','wr_rd_random_close','wr_close_rd_total','wr_close_rd_partial','wr_close_rd_random','wr_close_rd_total_close','wr_close_rd_partial_close','wr_close_rd_random_close']
# Basic test list
TST_BASIC=['readdir','xattr','link','symlink', 'rename','chmod','truncate','lock_posix_passing','lock_posix_blocking','crc32']
# Rebuild test list
TST_REBUILD=['gruyere','rebuild_fid','rebuild_one_dev','relocate_one_dev','rebuild_all_dev','rebuild_one_node','gruyere_reread']

ifnumber=get_if_nb()

list_cid=[]
list_sid=[]
list_host=[]

syslog.openlog("RozoTests",syslog.LOG_INFO)

(options, args) = parser.parse_args()
 
 
if options.rebuildCheck == True:  
  rebuildCheck=True 
else:
  rebuildCheck=False
     
if options.process != None:
  process=int(options.process)
  
if options.count != None:
  loop=int(options.count)
  
if options.fileSize != None:
  fileSize=int(options.fileSize)

if options.verbose == True:
  verbose=True

if options.list == True:
  do_list()
  exit(0)
    
if options.cont == True:  
  stopOnFailure=False 

if options.fusetrace == True:  
  stopOnFailure=True 
  fuseTrace=True

if options.speed == True:  
  loop=loop/4
  nbGruyere=nbGruyere/4
     
elif options.fast == True:  
  loop=loop/2
  nbGruyere=nbGruyere/2
   
elif options.long == True:  
  loop=loop*2 
  nbGruyere=nbGruyere*2

if options.mount == None: mnts="0"
else:               mnts=options.mount
  
  
# Snipper   
if options.snipper != None:
  instance=mnts.split(',')[0]
  resolve_mnt(instance)
  EXPORT_SID_NB,STORCLI_SID_NB=get_sid_nb()
  snipper(options.snipper)
  exit(0)  
  
#TST_REBUILD=TST_REBUILD+['rebuild_delete']

# Build list of test 
list=[] 
for arg in args:  
  if arg == "all":
    list.extend(TST_BASIC)
    list.extend(TST_REBUILD)
    list.extend(TST_RW)
    append_circumstance_test_list(list,TST_RW,'storageFailed')
    append_circumstance_test_list(list,TST_RW,'storageReset')
    if int(ifnumber) > int(1):
      append_circumstance_test_list(list,TST_RW,'ifUpDown')
       
#re    append_circumstance_test_list(list,TST_RW,'storcliReset')   
  elif arg == "rw":
    list.extend(TST_RW)
  elif arg == "storageFailed":
    append_circumstance_test_list(list,TST_RW,arg)
  elif arg == "storageReset":
    append_circumstance_test_list(list,TST_RW,arg)
  elif arg == "storcliReset":
    append_circumstance_test_list(list,TST_RW,arg)
  elif arg == "ifUpDown":
    append_circumstance_test_list(list,TST_RW,arg)   
  elif arg == "basic":
    list.extend(TST_BASIC)
  elif arg == "rebuild":
    list.extend(TST_REBUILD)  
  else:
    list.append(arg)              
# No list of test. Print usage
if len(list) == 0:
  usage()
  
new_list=[]    
if options.repeat != None:
  repeat = int(options.repeat)
  while repeat != int(0):
    new_list.extend(list)
    repeat=repeat-1
else:
  new_list.extend(list)  

do_compile_programs() 

for instance in mnts.split(','):
  resolve_mnt(instance)
  EXPORT_SID_NB,STORCLI_SID_NB=get_sid_nb()
  do_run_list(new_list)
