#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os.path
import subprocess
import time
import re
import shlex
from display_array import *

from optparse import OptionParser

fileSize=int(4)
loop=int(32)
process=int(8)
EXPORT_SID_NB=int(8)
STORCLI_SID_NB=int(8)
nbGruyere=int(1000)
stopOnFailure=False
fuseTrace=False

DEFAULT_RETRIES=int(20)
 

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
def get_sid_nb():
# Use debug interface to get the number of sid from exportd
#___________________________________________________

  string="./build/src/rozodiag/rozodiag -T mount:0:1 -c storaged_status"       
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
    if "UP" in line or "DOWN" in line:
      export_sid=export_sid+1

  return export_sid,storcli_sid   
#___________________________________________________
def export_count_sid_up ():
# Use debug interface to count the number of sid up 
# seen from the export. 
#___________________________________________________

  string="./build/src/rozodiag/rozodiag -T export -c vfstat_stor"
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  
  match=int(0)
  for line in cmd.stdout:
    if "UP" in line:
      match=match+1
    
  return match
#___________________________________________________
def get_rozofmount_instance ():

  p = subprocess.Popen(["ps","-ef"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for proc in p.stdout:
    if not "rozofsmount/rozofsmount" in proc:
      continue
      
    for words in proc.split():
      if mnt in words.split("/"):        
	for opt in proc.split(" "):
	  if opt.startswith("instance="):
            instance=opt.split("instance=")[1]
	    return int(instance)
	    
  return int(78)
#___________________________________________________
def storcli_count_sid_available ():
# Use debug interface to count the number of sid 
# available seen from the storcli. 
#___________________________________________________

#  inst=get_rozofmount_instance()
  string="./build/src/rozodiag/rozodiag -T mount:0:1 -c storaged_status"       
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
      print "Maximum retries reached. %s is %d"%(function,up)      
      return False
      
    sys.stdout.write(".")
    sys.stdout.flush()     
     
    up=getattr(sys.modules[__name__],function)()
    time.sleep(1)
    
  time.sleep(1)    
  return True  
    
#___________________________________________________
def wait_until_all_sid_up (retries=DEFAULT_RETRIES) :
# Wait for all sid up seen by storcli as well as export
#___________________________________________________

  if loop_wait_until(STORCLI_SID_NB,retries,'storcli_count_sid_available') == False:
    print "all sid are not up from storcli !!!"
    return False
  if loop_wait_until(EXPORT_SID_NB,retries,'export_count_sid_up') == False:
    print "all sid are not up from export !!!"
    return False
  return True  
  
    
#___________________________________________________
def wait_until_one_sid_down (retries=DEFAULT_RETRIES) :
# Wait until one sid down seen by storcli 
#___________________________________________________

  if loop_wait_until(STORCLI_SID_NB-1,retries,'storcli_count_sid_available') == False:
    print "all sid are not down from storcli !!!"
    return False
  return True   
#___________________________________________________
def storageStart (sid,count=int(1)) :

  while count != 0:
  
    sys.stdout.write("\rStorage %d restart"%(sid+1)) 
    sys.stdout.flush()
        
    os.system("./setup.sh storage %s start"%(sid+1))    

    if wait_until_all_sid_up() == True:
      return 0
    count=count-1
        
  return 1 
#___________________________________________________
def storageStop (sid) :
  
  sys.stdout.write("\r                                 ")
  sys.stdout.flush()  
  sys.stdout.write("\rStorage %d stop"%(sid+1))
  sys.stdout.flush()

  os.system("./setup.sh storage %s stop"%(sid+1))
  wait_until_one_sid_down()   
    
#___________________________________________________
def storageFailed (test) :
# Run test names <test> implemented in function <test>()
# under the circumstance that a storage is stopped
#___________________________________________________

  if wait_until_all_sid_up() == False:
    print "all sid are not up !!!"
    return 1

  for sid in range(STORCLI_SID_NB):

    storageStop(sid)
    reset_counters()
    
    try:
      # Resolve and call <test> function
      ret = getattr(sys.modules[__name__],test)()         
    except:
      ret = 1
      
    if ret != 0:
      return 1
      
    ret = storageStart(sid)  

      
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
      
      os.system("for process in `ps -ef  | grep \"storcli -i 1\" | grep -v storcli_starter.sh | grep -v grep | awk '{print $2}'`;do kill -9 $process;done")

      for i in range(7):
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
  string="./IT.py --snipper storcli"
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
def snipper_storio ():
# sub process that periodicaly resets the storio(s)
#___________________________________________________

  while True:
    for sid in range(STORCLI_SID_NB):

      if wait_until_all_sid_up() == False:
        return 1
      
      sys.stdout.write("\r                                 ")
      sys.stdout.flush()
      sys.stdout.write("\rStorio %d reset"%(sid+1))
      sys.stdout.flush()

      os.system("./setup.sh storio %d reset"%(sid+1))


#___________________________________________________
def storageReset (test):
# Run test names <test> implemented in function <test>()
# under the circumstance that storio(s) are periodicaly reset
#___________________________________________________
  global loop
  
 
  # Start process that reset the storages
  string="./IT.py --snipper storio"
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stderr=subprocess.PIPE)

  saveloop=loop
  loop=loop*5
  
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
    print "No such snipper target %s"%(target)
    ret = 1
  return ret  

#___________________________________________________
def wr_rd_total ():
#___________________________________________________

  do_compile_program('./rw')
  ret=os.system("./rw -process %d -loop %d -fileSize %d -total"%(process,loop,fileSize))
  return ret  

#___________________________________________________
def wr_rd_partial ():
#___________________________________________________

  do_compile_program('./rw')
  return os.system("./rw -process %d -loop %d -fileSize %d -partial"%(process,loop,fileSize))

#___________________________________________________
def wr_rd_random ():
#___________________________________________________

  do_compile_program('./rw')
  return os.system("./rw -process %d -loop %d -fileSize %d -random"%(process,loop,fileSize))

#___________________________________________________
def wr_rd_total_close ():
#___________________________________________________

  do_compile_program('./rw')
  return os.system("./rw -process %d -loop %d -fileSize %d -total -closeAfter"%(process,loop,fileSize))

#___________________________________________________
def wr_rd_partial_close ():
#___________________________________________________

  do_compile_program('./rw')
  return os.system("./rw -process %d -loop %d -fileSize %d -partial -closeAfter"%(process,loop,fileSize))

#___________________________________________________
def wr_rd_random_close ():
#___________________________________________________

  do_compile_program('./rw')
  return os.system("./rw -process %d -loop %d -fileSize %d -random -closeAfter"%(process,loop,fileSize))

#___________________________________________________
def wr_close_rd_total ():
#___________________________________________________

  do_compile_program('./rw')
  return os.system("./rw -process %d -loop %d -fileSize %d -total -closeBetween"%(process,loop,fileSize))

#___________________________________________________
def wr_close_rd_partial ():
#___________________________________________________

  do_compile_program('./rw')
  ret=os.system("./rw -process %d -loop %d -fileSize %d -partial -closeBetween"%(process,loop,fileSize))
  return ret 

#___________________________________________________
def wr_close_rd_random ():
#___________________________________________________

  do_compile_program('./rw')
  return os.system("./rw -process %d -loop %d -fileSize %d -random -closeBetween"%(process,loop,fileSize))

#___________________________________________________
def wr_close_rd_total_close ():
#___________________________________________________

  do_compile_program('./rw')
  return os.system("./rw -process %d -loop %d -fileSize %d -total -closeBetween -closeAfter"%(process,loop,fileSize))

#___________________________________________________
def wr_close_rd_partial_close ():
#___________________________________________________

  do_compile_program('./rw')
  return os.system("./rw -process %d -loop %d -fileSize %d -partial -closeBetween -closeAfter"%(process,loop,fileSize))

#___________________________________________________
def wr_close_rd_random_close ():
#___________________________________________________

  do_compile_program('./rw')
  return os.system("./rw -process %d -loop %d -fileSize %d -random -closeBetween -closeAfter"%(process,loop,fileSize))

#___________________________________________________
def rw2 ():
#___________________________________________________

  do_compile_program('./rw2')
  return os.system("./rw2 -loop %s -file mnt1_1/ze_rw2_test_file"%(loop))

#___________________________________________________
def prepare_file_to_read(filename,mega):
#___________________________________________________

  if not os.path.exists(filename):
    os.system("dd if=/dev/zero of=%s bs=1M count=%s 1> /dev/null"%(filename,mega))    

#___________________________________________________
def read_parallel ():
#___________________________________________________

  do_compile_program('./read_parallel')
  zefile='mnt1_1/myfile'
  prepare_file_to_read(zefile,fileSize) 
  ret=os.system("./read_parallel -process %s -loop %s -file %s"%(process,loop,zefile)) 
  return ret   

#___________________________________________________
def xattr():
#___________________________________________________

  do_compile_program('./test_xattr')  
  return os.system("./test_xattr -process %d -loop %d -mount mnt1_1"%(process,loop))

#___________________________________________________
def link():
#___________________________________________________

  do_compile_program('./test_link')  
  return os.system("./test_link -process %d -loop %d -mount mnt1_1"%(process,loop))

#___________________________________________________
def readdir():
#___________________________________________________

  do_compile_program('./test_readdir')  
  return os.system("./test_readdir -process %d -loop %d -mount mnt1_1"%(process,loop))

#___________________________________________________
def rename():
#___________________________________________________

  do_compile_program('./test_rename')  
  ret=os.system("./test_rename -process %d -loop %d -mount mnt1_1"%(process,loop))
  return ret 

#___________________________________________________
def chmod():
#___________________________________________________

  do_compile_program('./test_chmod')  
  return os.system("./test_chmod -process %d -loop %d -mount mnt1_1"%(process,loop))

#___________________________________________________
def truncate():
#___________________________________________________

  do_compile_program('./test_trunc')  
  return os.system("./test_trunc -process %d -loop %d -mount mnt1_1"%(process,loop))

#___________________________________________________
def lock_posix_passing():
#___________________________________________________

  do_compile_program('./test_file_lock')  
  zefile='mnt1_1/lock'
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./test_file_lock -process %d -loop %d -file %s -nonBlocking"%(process,loop,zefile))  

#___________________________________________________
def lock_posix_blocking():
#___________________________________________________

  do_compile_program('./test_file_lock')  
  zefile='mnt1_1/lock'
  try:
    os.remove(zefile)
  except:
    pass  
  ret=os.system("./test_file_lock -process %d -loop %d -file %s"%(process,loop,zefile))
  return ret 

#___________________________________________________
def lock_bsd_passing():
#___________________________________________________

  do_compile_program('./test_file_lock')  
  zefile='mnt1_1/lock'
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./test_file_lock -process %d -loop %d -file %s -nonBlocking -bsd"%(process,loop,zefile))

#___________________________________________________
def lock_bsd_blocking():
#___________________________________________________

  do_compile_program('./test_file_lock')  
  zefile='mnt1_1/lock'
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./test_file_lock -process %d -loop %d -file %s -nonBlocking"%(process,loop,zefile))  

#___________________________________________________
def gruyere_one_reread():
# reread files create by test_rebuild utility to check
# their content
#___________________________________________________

  do_compile_program('./test_rebuild')  
  return os.system("./test_rebuild -action check -nbfiles %d"%(int(nbGruyere)))

#___________________________________________________
def gruyere_reread():
# reread files create by test_rebuild with every storage
# possible fault to check every projection combination
#___________________________________________________

  ret = gruyere_one_reread()
  if ret != 0:
    return ret
  return storageFailed('gruyere_one_reread')

#___________________________________________________
def gruyere_write():
# Use test_rebuild utility to create a bunch of files
#___________________________________________________

  do_compile_program('./test_rebuild')  
  return os.system("./test_rebuild -action create -nbfiles %d"%(int(nbGruyere)))  

#___________________________________________________
def gruyere():
# call gruyere_write that create a bunch of files while
# block per block while storages are reset. This makes
# files with block dispersed on every storage. 
#___________________________________________________

  ret = storageReset('gruyere_write')
  if ret != 0:
    return ret
  return gruyere_reread()  

#___________________________________________________
def rebuild_one() :
# test rebuilding device per device
#___________________________________________________

  ret=1 
  for sid in range(STORCLI_SID_NB):
    for dev in range(6):
      os.system("./setup.sh storage %d device-delete %d 1> /dev/null"%(sid+1,dev))
      ret = os.system("./setup.sh storage %d device-rebuild %d 1> /dev/null"%(sid+1,dev))
      if ret != 0:
        return ret
      ret = gruyere_one_reread()  
      if ret != 0:
        return ret       
  return ret

#___________________________________________________
def rebuild_all() :
# test re-building a whole storage
#___________________________________________________

  ret=1 
  for sid in range(STORCLI_SID_NB):
    os.system("./setup.sh storage %d delete 1> /dev/null"%(sid+1))
    os.system("./setup.sh storage %d rebuild 1> /dev/null"%(sid+1))

    finish=False
    while finish == False:      
      time.sleep(4)      
      string="./build/src/rozodiag/rozodiag -i localhost%d -T storaged -c rebuild"%(sid+1)
      parsed = shlex.split(string)
      cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      for line in cmd.stdout:
        if "completed" in line:
	  finish = True
	  break

    ret = gruyere_one_reread()  
    if ret != 0:
      return ret    
       
  return ret
#___________________________________________________
def rebuild_fid() :
# test rebuilding per FID
#___________________________________________________

  for f in range(int(nbGruyere)/5):

    # Get the split of file on storages      
    string="./setup.sh cou mnt1_1/rebuild/%d"%(f+1)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # loop on the bins file constituting this file, and ask
    # the storages for a rebuild of the file
    for line in cmd.stdout:
      if ".bins" in line:
        words=line.split();
	if len(words) >= 2:
	
	  name=words[1].split('/')
	  fid=name[len(name)-1].split('.')[0]	  
	  dist=name[len(name)-2]
	  cidsid=name[len(name)-6].split("storage_")[1].split('-')	  
  
          string="./setup.sh storage %s fid-rebuild -s %s/%s -f 0/%s/%s "%(cidsid[1],cidsid[0],cidsid[1],dist,fid)
          parsed = shlex.split(string)
          cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
          cmd.wait()
	  
          if cmd.returncode != 0:
            return 1        

  return gruyere_one_reread()  

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

  if not os.path.exists(program) or os.stat(program).st_mtime < os.stat("%s.c"%(program)).st_mtime:
    os.system("gcc %s.c -lpthread -o %s -g"%(program,program))
     
#___________________________________________________
def do_run_list(list):
# run a list of test
#___________________________________________________

  tst_num=int(0)
  failed=int(0)
  success=int(0)
  
  dis = display_array(4)
  dis.new_line()  
  dis.set_column(1,'#')
  dis.set_column_on_right(1)  
  dis.set_column(2,'Name')
  dis.set_column(3,'Result')
  dis.set_column(4,'Duration')
  dis.set_column_on_right(4)  
  dis.separator()   

  time_start=time.time()
  
  total_tst=len(list)    
  for tst in list:
  
    tst_num=tst_num+1
    
    sys.stdout.write( "\r___%4d/%d : %s\n"%(tst_num,total_tst,tst))

    dis.new_line()  
    dis.set_column(1,'%s'%(tst_num))
    dis.set_column(2,tst)

    
    # Split optional circumstance and test name
    split=tst.split('/') 
    
    time_before=time.time()
    reset_counters()    
    
    if len(split) > 1:
    
      # There is a test circumstance. resolve and call the circumstance  
      # function giving it the test name
      try:
        ret = getattr(sys.modules[__name__],split[0])(split[1])          
      except:
        ret = 1

    else:

      # No test circumstance. Resolve and call the test function
      try:
        ret = getattr(sys.modules[__name__],split[0])()
      except:
        ret = 1
	
    delay=time.time()-time_before;	
    dis.set_column(4,'%s'%(my_duration(delay)))
    
    if ret == 0:
      dis.set_column(3,'OK')
      success=success+1
    else:
      dis.set_column(3,'FAILED')
      failed=failed+1
      
    if failed != 0 and stopOnFailure == True:
        break
         
    

  dis.separator()   
  dis.new_line()  
  dis.set_column(1,'%s'%(success+failed))
  dis.set_column(2,'Total')
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
  dis = display_array(3)
  dis.new_line()  
  dis.set_column(1,'Number')
  dis.set_column(2,'Test name')
  dis.set_column(3,'Test group')
  
  dis.separator()  
  for tst in TST_BASIC:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,tst)
    dis.set_column(3,'basic')  
    
  dis.separator()         
  for tst in TST_REBUILD:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,tst)
    dis.set_column(3,'rebuild') 
    
  dis.separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,tst)
    dis.set_column(3,'rw') 

  dis.separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,"%s/%s"%('storageFailed',tst))
    dis.set_column(3,'storageFailed') 

  dis.separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,"%s/%s"%('storageReset',tst))
    dis.set_column(3,'storageReset') 
    
  dis.separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,"%s/%s"%('storcliReset',tst))
    dis.set_column(3,'storcliReset')  

  dis.display()    

#___________________________________________________
def usage():
#___________________________________________________

  print "\n./IT.py -l"
  print "  Display the whole list of tests."
  print "\n./IT.py [options] [extra] <test name/group> [<test name/group>...]"      
  print "  Runs a test list."
  print "    options:"
  print "      [--fast]           The run 2 times faster tests."
  print "      [--long]           The run 2 times longer tests."
  print "      [--stop]           To stop the tests on the 1rst failure." 
  print "      [--fusetrace]      To enable fuse trace on test. When set, --stop is automaticaly set."
  print "    extra:"
  print "      [--process <nb>]   The number of processes that will run the test in paralell. (default %d)"%(process)
  print "      [--count <nb>]     The number of loop that each process will do. (default %s)"%(loop) 
  print "      [--fileSize <nb>]  The size in MB of the file for the test. (default %d)"%(fileSize)   
  print "      [--repeat <nb>]    The number of times the test list must be repeated."   
  print "    Test group and names can be displayed thanks to ./IT.py -l"
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
parser.add_option("-k","--snipper",action="store",type="string",dest="snipper", help="To start a storio/storcli snipper.")
parser.add_option("-s","--stop", action="store_true",dest="stop", default=False, help="To stop on 1rst failure.")
parser.add_option("-t","--fusetrace", action="store_true",dest="fusetrace", default=False, help="To stop on 1rst failure.")
parser.add_option("-F","--fast", action="store_true",dest="fast", default=False, help="To run 2 times faster tests.")
parser.add_option("-L","--long", action="store_true",dest="long", default=False, help="To run 2 times longer tests.")
parser.add_option("-r","--repeat", action="store", type="string", dest="repeat", help="Test repetition count.")

# Read/write test list
TST_RW=['read_parallel','rw2','wr_rd_total','wr_rd_partial','wr_rd_random','wr_rd_total_close','wr_rd_partial_close','wr_rd_random_close','wr_close_rd_total','wr_close_rd_partial','wr_close_rd_random','wr_close_rd_total_close','wr_close_rd_partial_close','wr_close_rd_random_close']
# Basic test list
TST_BASIC=['readdir','xattr','link','rename','chmod','truncate','lock_posix_passing','lock_posix_blocking']
# Rebuild test list
TST_REBUILD=['gruyere','rebuild_all']



(options, args) = parser.parse_args()

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
  
EXPORT_SID_NB,STORCLI_SID_NB=get_sid_nb()
  
if options.snipper != None:
  snipper(options.snipper)
  exit(0)  
    
if options.stop == True:  
  stopOnFailure=True 

if options.fusetrace == True:  
  stopOnFailure=True 
  fuseTrace=True
  
if options.fast == True:  
  loop=loop/2
  nbGruyere=nbGruyere/2
   
if options.long == True:  
  loop=loop*2 
  nbGruyere=nbGruyere*2
         

# Build list of test 
list=[] 
for arg in args:  
  if arg == "all":
    list.extend(TST_BASIC)
    list.extend(TST_REBUILD)
    list.extend(TST_RW)
    append_circumstance_test_list(list,TST_RW,'storageFailed')
    append_circumstance_test_list(list,TST_RW,'storageReset')
    append_circumstance_test_list(list,TST_RW,'storcliReset')   
  elif arg == "rw":
    list.extend(TST_RW)
  elif arg == "storageFailed":
    append_circumstance_test_list(list,TST_RW,arg)
  elif arg == "storageReset":
    append_circumstance_test_list(list,TST_RW,arg)
  elif arg == "storcliReset":
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
   
# Run the requested test list
do_run_list(new_list)
