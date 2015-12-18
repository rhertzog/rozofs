/*
  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
  This file is part of Rozofs.

  Rozofs is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation, version 2.

  Rozofs is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
 */


#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <libconfig.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/log.h>

#include "geomgr_config.h"

/* Settings names for storage configuration file */
#define GEOACTIVE   "active"
#define GEOEXPORTD  "export-daemons"
#define GEOHOST	    "host"
#define GEOEXPORT   "exports"
#define GEOSITE     "site"
#define GEOPATH     "path"
#define GEOCALENDAR "calendar"
#define GEOSTART    "start"
#define GEOSTOP     "stop"
#define GEONB       "nb"


geomgr_config_calendar_t * geomgr_config_calendar_initialize(geomgr_config_export_t * export,
                                                   int nb_records) {
  geomgr_config_calendar_t * c;
  int                           size;

  DEBUG_FUNCTION;

  if (nb_records <= 0) return NULL;
  
  size = sizeof(geomgr_config_calendar_t)+((nb_records-1)*sizeof(geomgr_config_calendar_entry_t));

  c = xmalloc(size);
  memset(c,0,size);
  export->calendar = c;

  return c;
}
geomgr_config_export_t * geomgr_config_export_initialize(geomgr_config_exportd_t * exportd,
                                                   int                  site,
						   const char         * path) {
  geomgr_config_export_t * e;
  int                           size;
  DEBUG_FUNCTION;

  size = sizeof(geomgr_config_export_t);

  e = xmalloc(size);
  list_init(&e->list);
  
  e->site = site;
  strcpy(e->path,path);
  e->calendar = NULL;
  list_push_back(&exportd->export,&e->list);

  return e;
}

geomgr_config_t * geomgr_config_initialize(int nb_exportd) {
  geomgr_config_t * cfg;
  int            size=0;
  DEBUG_FUNCTION;

  if (nb_exportd <= 0) return NULL;
  
  size = sizeof(geomgr_config_t)+((nb_exportd-1)*sizeof(geomgr_config_exportd_t));

  cfg = xmalloc(size);
  memset(cfg,0,size);

  return cfg;
}

void geomgr_config_release(geomgr_config_t *cfg) { 
  int exportd;
  list_t *p, *q;
  
  for (exportd=0; exportd < cfg->nb_exportd; exportd++) {

    list_for_each_forward_safe(p, q, &cfg->exportd[exportd].export) {
      geomgr_config_export_t *entry = list_entry(p, geomgr_config_export_t, list);
      if (entry->calendar != NULL) {
        free(entry->calendar);
	entry->calendar = NULL;
      }
      list_remove(p);
      free(entry);
    }
  }
  
  free(cfg); 
}


geomgr_config_t * geomgr_config_read(const char *fname) {
    geomgr_config_t * config = NULL;
    geomgr_config_exportd_t * pExportd=NULL;
    config_t cfg;
    struct config_setting_t *exportd_settings = 0;
    struct config_setting_t *exports_settings = 0;
    int    nb_exportd=0;
    int    nb_exports=0;
    int    nb_records=0;
    int i = 0;
    int j = 0;
    int k = 0;
    const char * valstring;
    geomgr_config_export_t *pExport=NULL;    
    geomgr_config_calendar_t * calendar=NULL;
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
               || (LIBCONFIG_VER_MAJOR > 1))
    int valint;
    
#else
    long int valint;
#endif  
    int status;   
    DEBUG_FUNCTION;
      
    if (access(fname,R_OK) == -1) {
      return NULL;
    }
    
    config_init(&cfg);
    
          
    if (config_read_file(&cfg, fname) == CONFIG_FALSE) {
      severe("can't read %s line %d: %s.", fname, config_error_line(&cfg), config_error_text(&cfg));
      goto out;
    }

    
    status = 1;    
    config_lookup_bool(&cfg, GEOACTIVE, &status);
    
    if (!(exportd_settings = config_lookup(&cfg, GEOEXPORTD))) {
      // No export daemon declared
      goto out;
    }

    nb_exportd = config_setting_length(exportd_settings);
    if (nb_exportd == 0) {
      // No export daemon declared
      goto out;      
    }
    config = geomgr_config_initialize(nb_exportd);
    config->active = status;
    

    for (i = 0; i < nb_exportd; i++) {
    
        struct config_setting_t * exportd = NULL;

        if (!(exportd = config_setting_get_elem(exportd_settings, i))) {
            errno = ENOKEY;
            severe("can't fetch exportd settings %d.", i);
            goto out;
        }

        status = 1;
        config_setting_lookup_bool(exportd, GEOACTIVE, &status);
	

        if (config_setting_lookup_string(exportd, GEOHOST, &valstring)
                == CONFIG_FALSE) {
            severe("can't lookup host in exportd %d.", i);
            goto out;
        }
        if (strlen(valstring) > GEO_STRING_MAX) {
            severe("host string too long(%d/%d) in exportd %d.",
	            (int) strlen(valstring), GEO_STRING_MAX, i);
            continue;
	}	


	if (!(exports_settings = config_setting_get_member(exportd, GEOEXPORT))) {
	  // No exports on this daemon
	  continue;
	}

	nb_exports = config_setting_length(exports_settings);
	if (nb_exports == 0) {
	  // No exports on this daemon
	  continue;     
	}
	
        pExportd = &config->exportd[config->nb_exportd++];
	pExportd->active = status * config->active;
	strcpy(pExportd->host,valstring);
	list_init(&pExportd->export);
	

	for (j = 0; j < nb_exports; j++) {

            struct config_setting_t * export = NULL;
            struct config_setting_t * calendar_settings = NULL;
	    int hour, min;

            if (!(export = config_setting_get_elem(exports_settings, j))) {
        	severe("can't fetch exports settings %d in exportd %d.", j, i);
        	break;
            }

            status = 1;
            config_setting_lookup_bool(export, GEOACTIVE, &status);
	    

            if (config_setting_lookup_int(export, GEOSITE, &valint)
                    == CONFIG_FALSE) {
        	severe("can't lookup site in export %d of exportd %d.", j, i);
        	continue;
	    }
            if ((valint !=0)&&(valint !=1)) {
        	severe("site value %d in export %d of exportd %d.",valint, j, i);
        	continue;
	    }

            if (config_setting_lookup_string(export, GEOPATH, &valstring)
                    == CONFIG_FALSE) {
        	severe("can't lookup path in export %d of exportd %d.", j, i);
        	continue;
            }
            if (strlen(valstring) > GEO_STRING_MAX) {
        	severe("path string too long(%d/%d) in export %d of exportd %d.",
	        	(int) strlen(valstring), GEO_STRING_MAX, j, i);
        	continue;
	    }	
	    
	    pExport = geomgr_config_export_initialize(pExportd,valint,valstring);
	    pExport->active = status * pExportd->active;
	    

            valint = 1;
            config_setting_lookup_int(export, GEONB, &valint);
            if ((valint < 1)||(valint >4)) {
              severe("nb instance value %d should be within [1:4] in export %d of exportd %d.",valint, j, i);
	      valint = 1;
	    }
	    pExport->nb = valint;
	    
	    
	    if (!(calendar_settings = config_setting_get_member(export, GEOCALENDAR))) {
	      // No calendar
	      continue;
	    }
	    
	    nb_records = config_setting_length(calendar_settings);
            calendar = geomgr_config_calendar_initialize(pExport,nb_records);
	    
	    for (k=0; k < nb_records; k++) {
              struct config_setting_t * interval = NULL;

              if (!(interval = config_setting_get_elem(calendar_settings, k))) {
        	  severe("can't fetch calendar entry %d of export %d in exportd %d.", k, j, i);
        	  continue;
              }
	      
              if (config_setting_lookup_string(interval, GEOSTART, &valstring)
                      == CONFIG_FALSE) {
        	  severe("can't lookup start in calendar entry %d in export %d of exportd %d.", k, j, i);
        	  continue;
	      }	 
	      if (sscanf(valstring,"%d:%d",&hour,&min) != 2) {
        	  severe("Bad start value in calendar entry %d in export %d of exportd %d.", k, j, i);
        	  continue;
	      } 
	      if ((hour<0)||(hour>24)) {
        	  severe("Invalid start hours %d in calendar entry %d in export %d of exportd %d.", hour, k, j, i);
        	  continue;
	      }
	      if ((min<0)||(min>60)) {
        	  severe("Invalid start minutes %d in calendar entry %d in export %d of exportd %d.", min, k, j, i);
        	  continue;
	      }	       	      
	      calendar->entries[calendar->nb_entries].start = hour*60+min;
	          
              if (config_setting_lookup_string(interval, GEOSTOP, &valstring)
                      == CONFIG_FALSE) {
        	  severe("can't lookup stop in calendar entry %d in export %d of exportd %d.", k, j, i);		      
		  continue;
	      }		      
	      if (sscanf(valstring,"%d:%d",&hour,&min) != 2) {
        	  severe("Bad stop value in calendar entry %d in export %d of exportd %d.", k, j, i);
		  continue;		    
	      }	
	      if ((hour<0)||(hour>24)) {
        	  severe("Invalid stop hours %d in calendar entry %d in export %d of exportd %d.", hour, k, j, i);
        	  continue;
	      }
	      if ((min<0)||(min>60)) {
        	  severe("Invalid stop minutes %d in calendar entry %d in export %d of exportd %d.", min, k, j, i);
        	  continue;
	      }		           
	      calendar->entries[calendar->nb_entries++].stop = hour*60+min;  	      
	    }	    
	    
	}
    }

out:
    config_destroy(&cfg);
    return config;
}

