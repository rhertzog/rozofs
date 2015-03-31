/*
** To be put in export.h
*/

#define MAX_SLICE_BIT 8
#define MAX_SLICE_NB (1<<MAX_SLICE_BIT)
#define MAX_SUBSLICE_BIT 12
#define MAX_SUBSLICE_NB (1<<MAX_SUBSLICE_BIT)
/*
**__________________________________________________________________
*/
static inline void mstor_get_slice_and_subslice(fid_t fid,uint32_t *slice,uint32_t *subslice) 
{
    uint32_t hash = 0;
    uint8_t *c = 0;
    
    for (c = fid; c != fid + 16; c++)
        hash = *c + (hash << 6) + (hash << 16) - hash;
    
    *slice = hash & ((1<<MAX_SLICE_BIT) -1);
    hash = hash >> MAX_SLICE_BIT;
    *subslice = hash & ((1<<MAX_SUBSLICE_BIT) -1);
}

/*
** end of export.h
*/

/*
**__________________________________________________________________
*/
/** get the lv1 directory.
 *
 * lv1 entries are first level directories of an export root named by uint32_t
 * string value and used has entry of a hash table storing the export
 * meta data files.
 *
 * @param root_path: root path of the exportd
 * @param slice: value of the slice
 *
 * @return 0 on success otherwise -1
 */
static inline int mstor_slice_resolve_entry(char *root_path,uint32_t slice) {
    char path[PATH_MAX];
    sprintf(path, "%s/%"PRId32"", root_path, slice);
    if (access(path, F_OK) == -1) {
        if (errno == ENOENT) 
        {
          /*
          ** it is the fisrt time we access to the slice
          **  we need to create the level 1 directory and the 
          ** timestamp file
          */
          if (mkdir(path, S_IRUSR | S_IWUSR | S_IXUSR) != 0)
          {
            severe("mkdir (%s): %s",path,strerror(errno));
            return -1;
          }                
//          mstor_ts_srv_create_slice_timestamp_file(export,slice); 
          return 0;         
        } 
        /*
        ** any other error
        */
        severe("access (%s): %s",path,strerror(errno));
        return -1;
    }
    return 0;
}


/*
**__________________________________________________________________
*/
/** get the subslice directory index.
 *
 * lv1 entries are first level directories of an export root named by uint32_t
 * string value and used has entry of a hash table storing the export
 * meta data files.
 *
 * @param root_path: root path of the exportd
 * @param fid: the search fid
 *
 * @return 0 on success otherwise -1
 */
static inline int mstor_subslice_resolve_entry(char *root_path, fid_t fid,uint32_t slice,uint32_t subslice) 
{
    char path[PATH_MAX];


    sprintf(path, "%s/%d/%d",root_path, slice,subslice);
    if (access(path, F_OK) == -1) {
        if (errno == ENOENT) 
        {
          /*
          ** it is the fisrt time we access to the subslice
          **  we need to create the associated directory 
          */
          if (mkdir(path, S_IRUSR | S_IWUSR | S_IXUSR) != 0)
          {
            severe("mkdir (%s): %s",path,strerror(errno));
            return -1;
          }
          return 0;
        } 
        severe("access (%s): %s",path,strerror(errno));
        return -1;
    }
    return 0;
}

/** build a full path based on export root and fid of the lv2 file
 *
 * lv2 is the second level of files or directories in storage of metadata
 * they are acceded thru mreg or mdir API according to their type.
 *
 * @param root_path: root path of the exportd
 * @param fid: the fid we are looking for
 * @param path: the path to fill in
 */

static inline int export_lv2_resolve_path_internal(char *root_path, fid_t fid, char *path) 
{
    uint32_t slice;
    uint32_t subslice;
    char str[37];
    
    /*
    ** extract the slice and subsclie from the fid
    */
    mstor_get_slice_and_subslice(fid,&slice,&subslice);
    /*
    ** check the existence of the slice directory: create it if it does not exist
    */
    if (mstor_slice_resolve_entry(root_path,slice) < 0)
    {
        goto error;
    }
    /*
    ** check the existence of the subslice directory: create it if it does not exist
    */
    if (mstor_subslice_resolve_entry(root_path, fid,slice,subslice) < 0)
    {
        goto error;
    }
    /*
    ** convert the fid in ascii
    */
    rozofs_uuid_unparse(fid, str);
    sprintf(path, "%s/%d/%d/%s", root_path, slice,subslice,str);
    return 0;
    
error:
    return -1;
}



/** build a full path based on export root and fid of the lv2 file
 *
 * lv2 is the second level of files or directories in storage of metadata
 * they are acceded thru mreg or mdir API according to their type.
 *
 * @param export: the export we are searching on
 * @param fid: the fid we are looking for
 * @param path: the path to fill in
 */

int export_lv2_resolve_path(export_t *export, fid_t fid, char *path) 
{
    int ret;

    START_PROFILING(export_lv2_resolve_path);
    
    ret = export_lv2_resolve_path_internal(export->root,fid,path);

    STOP_PROFILING(export_lv2_resolve_path);
    return ret;
}
