/*
  MyFS. One directory, one file, 1000 bytes of storage. What more do you need?
  
  This Fuse file system is based largely on the HelloWorld example by Miklos Szeredi <miklos@szeredi.hu> (http://fuse.sourceforge.net/helloworld.html). Additional inspiration was taken from Joseph J. Pfeiffer's "Writing a FUSE Filesystem: a Tutorial" (http://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/).
*/

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <errno.h>
#include <fcntl.h>

#include "myfs.h"

// The one and only fcb that this implmentation will have. We'll keep it in memory. A better
// implementation would, at the very least, cache it's root directroy in memory.
myfcb the_root_fcb;
unqlite_int64 root_object_size_value = sizeof(myfcb);

// This is the pointer to the database we will use to store all our files
unqlite *pDb;
uuid_t zero_uuid;

//Functions following can be used for shorten the code and improve the stroage usage:

//Splite path can divide the path into arbitary length
char** split_path(const char* path, int* size) {
	char* cpy_path = strdup(path);
	char** path_lst;

	//set pointer
	char* ptr = cpy_path;
	while (*ptr != '\0') {
		if ('/' == *ptr) {
			(*size)++;
		}
		ptr++;
	}
	char** path_arr = malloc(sizeof(char*) * ((*size) - 1));
	if (path_arr) {
		//Set token dividied by "/".
		//Since the file format is "/.../.../..." so we abandon the first token - "/"
		char *token = strtok(cpy_path, "/");
		int id = 0;
		//now the path is .../.../...
		//By using second strtok we will get: token:...  -> non_cons:.../...
		//Now the pointer array will create like [hier0, hier1, ..., hier(n-2)] and the final destination is path_arr[count - 2]
		for (int i = 0; i < (*size) - 1; i++) {
			*(path_arr + i) = strdup(strtok(0, "/"));
		}
	}
	free(cpy_path);
	return path_lst;
}

void free_strptr_arr(char** arr_ptr, int size) {
	for (int i = 0; i < size; i++) {
		free(arr_ptr[i]);
	}
}

//Direct: OK
//Single indirect : OK
//Doudble indirect: ?
int directory_uuid_getter(access_block dir, uuid_t **uuid_arr) {
	uuid_t* uuid_list = malloc(sizeof(uuid_t) * dir.size);
	int rc;
	//Direct access
	for (int i = 0; i < DIRECT_SIZE; i++) {
		if (uuid_compare(dir.direct_access[i], zero_uuid) == 0) {
			continue;
		}
		uuid_copy(uuid_list[i], dir.direct_access[i]);
	}
	//Single indirect access
	if (uuid_compare(dir.single_indirect, zero_uuid) != 0) {
		unqlite_int64 nBytes;
		indirect tmp;
		rc = unqlite_kv_fetch(pDb, &(dir.single_indirect), KEY_SIZE, &tmp, &nBytes);
		if (rc != UNQLITE_OK) {
			error_handler(rc);
			return -EIO;
		}
		for (int i = 0; i < INDIRECT_SIZE; i++) {
			if (uuid_compare(tmp.indirect_access[i], zero_uuid) == 0) {
				continue;
			}
			uuid_copy(uuid_list[DIRECT_SIZE + i], tmp.indirect_access[i]);
		}
	}
	//Assign uuid_list to incoming variable
	*uuid_arr = uuid_list;
	return 0;
}

//Read the specific filename from directory block and assign to fcb
//This currently only works on direct access only
int readPath(char* filename, access_block dir, myfcb *fcb) {
	uuid_t* uuid_list;
	if (directory_uuid_getter(dir, &uuid_list) == 0) {
		int rc;
		unqlite_int64 nBytes;
		myfcb tmp;
		for (int i = 0; i < dir.size; i++) {
			rc = unqlite_kv_fetch(pDb, &(uuid_list[i]), KEY_SIZE, &tmp, &nBytes);
			if (rc != UNQLITE_OK) {
				free(uuid_list);
				error_handler(rc);
				return -EIO;
			}
			if (strcmp(tmp.path, filename) == 0) {
				free(uuid_list);
				*fcb = tmp;
				return 0;
			}
		}
		free(uuid_list);
		return -ENONET;
	}
	else {
		write_log("read_path: read failed with unknown exception.\n");
		return -EIO;
	}
}

//Generate a new key from the dierctory block
int generate_newkey(access_block* dir, uuid_t* key) {
	//Direct access
	for (int i = 0; i < DIRECT_SIZE; i++) {
		if (uuid_compare(zero_uuid, dir->direct_access[i]) == 0) {
			uuid_generate(dir->direct_access[i]);
			uuid_copy(*key, dir->direct_access[i]);
			return 0;
		}
	}

	//Single indirect access
	int rc;
	//if indirect is 0, generate a new key.
	if (uuid_compare(zero_uuid, dir->single_indirect) == 0) {
		indirect indir;
		memset(&indir, 0, sizeof(indirect)); //Clear everything in indir
		

		uuid_generate(dir -> single_indirect); //generate indirect key
		
	}
	else {

	}

	write_log("generateor_new_key(): Directory space is not enough.\n");
	return -EIO;
}

//Find free access path from access_block
//With free_list this will read the free_list first, or generate a new fcb
int find_free(myfcb src, uuid_t *key) {
	int rc;
	access_block s_dir;
	unqlite_int64 nBytes;

	//Now we are currently looking at s_dir block
	rc = unqlite_kv_fetch(pDb, &(src.file_data_id), KEY_SIZE, &s_dir, &nBytes);
	if (rc != UNQLITE_OK || nBytes != sizeof(access_block)) {
		write_log("find_free(): fetch directory content from unqlite failed: %i - size read: %i\n", rc, nBytes);
		error_handler(rc);
		return -EIO;
	}

	if ((rc = generate_newkey(&s_dir, key)) != 0) {
		return rc;
	}

	//Now we should write back changes and create a new fcb.
	rc = unqlite_kv_store(pDb, &(src.file_data_id), KEY_SIZE, &s_dir, sizeof(access_block));
	if (rc != UNQLITE_OK) {
		write_log("find_free(): Write back failed: %i \n", rc);
		error_handler(rc);
		return -EIO;
	}
	return 0;
}

//This will accept an file id and set the directory in the fcb.
int create_new_dir(uuid_t* file_id, uuid_t* dir_id, mode_t mode) {
	//Set directory block and fcb
	access_block *new_dir;
	myfcb *new_fcb;

	//initial directory control block with 0
	memset(new_dir, 0, sizeof(access_block));
	//initial file control block with 0
	memset(new_fcb, 0, sizeof(myfcb));
	//generate new file_data_id
	uuid_t dir_uuid;
	uuid_t fcb_uuid;
	
	//Set the fcb
	new_fcb -> atime = time(NULL);
	new_fcb -> mtime = time(NULL);
	new_fcb -> ctime = time(NULL);
	new_fcb -> uid = getuid();
	new_fcb -> gid = getgid();
	new_fcb -> mode = mode | S_IFDIR;
	new_fcb -> size = sizeof(myfcb);
	//link new_dir and new_fcb
	uuid_copy(new_fcb -> file_data_id, *dir_id);
	
	int rc;
	//write back fcb
	rc = unqlite_kv_store(pDb, file_id, KEY_SIZE, new_fcb, sizeof(myfcb));
	if (rc != UNQLITE_OK) {
		write_log("create_dir: Create failed - failed to write fcb into unqlite: %i\n", rc);
		error_handler(rc);
		return -EIO;
	}

	//write back directory
	rc = unqlite_kv_store(pDb, dir_id, KEY_SIZE, new_dir, sizeof(access_block));
	if (rc != UNQLITE_OK) {
		write_log("create_dir: Create failed - failed to write directory into unqlite: %i\n", rc);
		error_handler(rc);
		return -EIO;
	}
	return 0;
}


// The functions which follow are handler functions for various things a filesystem needs to do:
// reading, getting attributes, truncating, etc. They will be called by FUSE whenever it needs
// your filesystem to do something, so this is where functionality goes.

// Get file and directory attributes (meta-data).
// Read 'man 2 stat' and 'man 2 chmod'.
static int myfs_getattr(const char *path, struct stat *stbuf)
{

	write_log("myfs_getattr(path=\"%s\", statbuf=0x%08x)\n", path, stbuf);

	memset(stbuf, 0, sizeof(struct stat));

	myfcb s_file = the_root_fcb; 	//fcb that needs to read
	access_block directory;		 	//directory block

	//if path is the root directory, we do not need to fetch the unqlite.
	if (strcmp(path, "/") == 0)
	{
		stbuf->st_mode = the_root_fcb.mode;
		stbuf->st_nlink = 2;
		stbuf->st_mtime = the_root_fcb.mtime;
		stbuf->st_ctime = the_root_fcb.ctime;
		stbuf->st_size = the_root_fcb.size;
		stbuf->st_uid = the_root_fcb.uid;
		stbuf->st_gid = the_root_fcb.gid;
	}
	else
	{
		int rc; //unqlite statement code
		int hier_len;
		char** path_arr = split_path(path, &hier_len); //hier_len - 1 is the hierarchy level of the file and path_arr includes the hierarchy content of the path
		
		//reach to the final fcb
		if(path_arr) {
			for (int i = 0; i < hier_len - 1; i++) {
				char* next = path_arr[i];
				unqlite_int64 nBytes;

				rc = unqlite_kv_fetch(pDb, &(s_file.file_data_id), KEY_SIZE, &directory, &nBytes);
				if (rc != UNQLITE_OK) {
					error_handler(rc);
					return -EIO;
				}
				myfcb tmp_fcb; //FCB
				char found = 0;
				if (directory.size <= DIRECT_SIZE) {
					for (int j = 0; j < directory.size; j++) {
						uuid_t* current = directory.direct_access;
						rc = unqlite_kv_fetch(pDb, &(current[j]), KEY_SIZE, &tmp_fcb, &nBytes); //This will fetch fcb of current uid
						if (rc != UNQLITE_OK) {
							error_handler(rc);
							return -EIO;
						}
						if (strcmp(tmp_fcb.path, next) == 0) {
							found = 1;
							s_file = tmp_fcb;
							break;
						}
					}
				}
				if (found == 0) {
					write_log("File not found.\n");
					return -ENOENT;
				}
			}
		}
		else {
			write_log("myfs: getattr() memory not enough\n");
		}
		//Print state
		stbuf->st_mode = s_file.mode;
		stbuf->st_nlink = 2; //Not defined
		stbuf->st_mtime = s_file.mtime;
		stbuf->st_ctime = s_file.ctime;
		stbuf->st_size = s_file.size;
		stbuf->st_uid = s_file.uid;
		stbuf->st_gid = s_file.gid;
		free_strptr_arr(path_arr, hier_len);
	}
	return 0;
}

// Read 'man 2 readdir'.
/* Test_log: 
	Root directory: OK
	Directories:
 */
static int myfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	int rc; //statement

	(void)offset; // This prevents compiler warnings
	(void)fi;

	//Define the file control block and directory block
	access_block directory;

	//Operation log
	write_log("write_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n", path, buf, filler, offset, fi);

	// We always output . and .. first, by convention. See documentation for more info on filler()
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	//If the path is the root path: 
	if (strcmp(path, "/") == 0) {
		unqlite_int64 nBytes;
		rc = unqlite_kv_fetch(pDb, &(the_root_fcb.file_data_id), KEY_SIZE, &directory, &nBytes);
		if (rc != UNQLITE_OK) error_handler(rc);
		if (nBytes != sizeof(access_block)) {
			write_log("myfs_readdir: Size of access_block invalid");
			return -EIO;
		}
		return 0; //Success
	}
	else {
		myfcb s_file; 				//Define the pointer
		s_file = the_root_fcb;		//assign the pointer to the root directory
		char *tmp;					//Reach to the directory
		char *ptr;					//pointer to &tmp
		int count = 0;				//Counting for hierarchy
		tmp = strdup(path); 		//Copy constant string to non-constant with memcpy, can be achieved with strdup()
		ptr = tmp;					//Set pointer to tmp
		
		// STEP 1 - divide path into arrays:
		// 		  - Creating an array for storing hierarchy directory struct
		// 		  - 	e.g. ls -la /..1/..2/..3/ There will be 3 hierarchy directory
		while (*ptr != '\0') {
			if ('/' == *ptr) {
				count++;
			}
			ptr++;
		}
		write_log("myfs_readdir: hierarchy amount is %i\n", count - 1);  //Debugging <=

		// count is the number of /, We can now malloc count -1 for storing pointer arrays
		char** path_arr = malloc(sizeof(char*) * (count - 1));

		if (path_arr) {
			//Set token dividied by "/".
			//Since the file format is "/.../.../..." so we abandon the first token - "/"
			char *token = strtok(tmp, "/");
			int id = 0;
			//now the path is .../.../...
			//By using second strtok we will get: token:...  -> non_cons:.../...
			//Now the pointer array will create like [hier0, hier1, ..., hier(n-2)] and the final destination is path_arr[count - 2]
			for (int i = 0; i < count - 1; i++) {
				*(path_arr + i) = strdup(strtok(0, "/"));
			}
		}
		else {
			write_log("myfs_readdir: Memory failed. \n");
			return -ENOENT;
		}
		//Then read through the hierarchy to get the data from unqlite
		//We may need to attention that whether it is a directory
		for (int i = 0; i < count - 1; i++) {
			char* next = path_arr[i];
			unqlite_int64 nBytes;		//nBytes;

			rc = unqlite_kv_fetch(pDb, &(s_file.file_data_id), KEY_SIZE, &directory, &nBytes); //This will fetch the directory information. It should success.
			if (rc != UNQLITE_OK) {
				error_handler(rc);
				return -EIO;
			}
			//Now the directory has the current directory information
			//Then we will try to reach to next hierarchy
			//First we will not concentrate on indirect entry
			myfcb tmp_fcb;
			char found = 0;
			if (directory.size <= DIRECT_SIZE) {
				for (int j = 0; j < directory.size; j++) {
					uuid_t* current = directory.direct_access;
					rc = unqlite_kv_fetch(pDb, &(current[j]), KEY_SIZE, &tmp_fcb, &nBytes); //This will fetch fcb of current uid
					if (rc != UNQLITE_OK) {
						error_handler(rc);
						return -EIO;
					}
					if (strcmp(tmp_fcb.path, next) == 0) {
						//Judge it is an directory
						if (tmp_fcb.mode == S_IFDIR) {
							found = 1;
							s_file = tmp_fcb;
							break;
						}
					}
				}
			}
			if (found == 0) {
				write_log("File not found.");
				return -ENOENT;
			}
		}
		//Free path array and strduped filepath
		free_strptr_arr(path_arr, count - 1);
		free(tmp);
	}

	myfcb tmp_fcb;
	unqlite_int64 nBytes;
	//Read all data in dir block -> direct access only
	for (int i = 0; i < directory.size; i++) {
		uuid_t* current = directory.direct_access;
		rc = unqlite_kv_fetch(pDb, &(current[i]), KEY_SIZE, &tmp_fcb, &nBytes); //This will fetch fcb of current uid
		if (rc != UNQLITE_OK) {
			error_handler(rc);
			return -EIO;
		}
		filler(buf, tmp_fcb.path, NULL, 0);
	}
	return 0;
}

// Read a file.
// Read 'man 2 read'.
static int myfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	size_t len;
	(void)fi;

	write_log("myfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n", path, buf, size, offset, fi);

	if (strcmp(path, the_root_fcb.path) != 0)
	{
		write_log("myfs_read - ENOENT");
		return -ENOENT;
	}

	len = the_root_fcb.size;

	uint8_t data_block[MY_MAX_FILE_SIZE];

	memset(&data_block, 0, MY_MAX_FILE_SIZE);
	uuid_t *data_id = &(the_root_fcb.file_data_id);
	// Is there a data block?
	if (uuid_compare(zero_uuid, *data_id) != 0)
	{
		unqlite_int64 nBytes; //Data length.
		int rc = unqlite_kv_fetch(pDb, data_id, KEY_SIZE, NULL, &nBytes);
		if (rc != UNQLITE_OK)
		{
			error_handler(rc);
		}
		if (nBytes != MY_MAX_FILE_SIZE)
		{
			write_log("myfs_read - EIO");
			return -EIO;
		}

		// Fetch the fcb the root data block from the store.
		unqlite_kv_fetch(pDb, data_id, KEY_SIZE, &data_block, &nBytes);
	}

	if (offset < len)
	{
		if (offset + size > len)
			size = len - offset;
		memcpy(buf, &data_block + offset, size);
	}
	else
		size = 0;

	return size;
}

// This file system only supports one file. Create should fail if a file has been created. Path must be '/<something>'.
// Read 'man 2 creat'.
static int myfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	write_log("myfs_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n", path, mode, fi);

	if (the_root_fcb.path[0] != '\0')
	{
		write_log("myfs_create - ENOSPC");
		return -ENOSPC;
	}

	int pathlen = strlen(path);
	if (pathlen >= MY_MAX_PATH)
	{
		write_log("myfs_create - ENAMETOOLONG");
		return -ENAMETOOLONG;
	}
	sprintf(the_root_fcb.path, path);
	struct fuse_context *context = fuse_get_context();
	the_root_fcb.uid = context->uid;
	the_root_fcb.gid = context->gid;
	the_root_fcb.mode = mode | S_IFREG;

	int rc = unqlite_kv_store(pDb, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE, &the_root_fcb, sizeof(myfcb));
	if (rc != UNQLITE_OK)
	{
		write_log("myfs_create - EIO");
		return -EIO;
	}

	return 0;
}

// Set update the times (actime, modtime) for a file. This FS only supports modtime.
// Read 'man 2 utime'.
static int myfs_utime(const char *path, struct utimbuf *ubuf)
{
	write_log("myfs_utime(path=\"%s\", ubuf=0x%08x)\n", path, ubuf);

	if (strcmp(path, the_root_fcb.path) != 0)
	{
		write_log("myfs_utime - ENOENT");
		return -ENOENT;
	}
	the_root_fcb.mtime = ubuf->modtime;

	// Write the fcb to the store.
	int rc = unqlite_kv_store(pDb, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE, &the_root_fcb, sizeof(myfcb));
	if (rc != UNQLITE_OK)
	{
		write_log("myfs_write - EIO");
		return -EIO;
	}

	return 0;
}

// Write to a file.
// Read 'man 2 write'
static int myfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	write_log("myfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n", path, buf, size, offset, fi);

	if (strcmp(path, the_root_fcb.path) != 0)
	{
		write_log("myfs_write - ENOENT");
		return -ENOENT;
	}

	if (size >= MY_MAX_FILE_SIZE)
	{
		write_log("myfs_write - EFBIG");
		return -EFBIG;
	}

	uint8_t data_block[MY_MAX_FILE_SIZE];

	memset(&data_block, 0, MY_MAX_FILE_SIZE);
	uuid_t *data_id = &(the_root_fcb.file_data_id);
	// Is there a data block?
	if (uuid_compare(zero_uuid, *data_id) == 0)
	{
		// GEnerate a UUID fo rhte data blocl. We'll write the block itself later.
		uuid_generate(the_root_fcb.file_data_id);
	}
	else
	{
		// First we will check the size of the obejct in the store to ensure that we won't overflow the buffer.
		unqlite_int64 nBytes; // Data length.
		int rc = unqlite_kv_fetch(pDb, data_id, KEY_SIZE, NULL, &nBytes);
		if (rc != UNQLITE_OK || nBytes != MY_MAX_FILE_SIZE)
		{
			write_log("myfs_write - EIO");
			return -EIO;
		}

		// Fetch the data block from the store.
		unqlite_kv_fetch(pDb, data_id, KEY_SIZE, &data_block, &nBytes);
		// Error handling?
	}

	// Write the data in-memory.
	int written = snprintf(data_block, MY_MAX_FILE_SIZE, buf);

	// Write the data block to the store.
	int rc = unqlite_kv_store(pDb, data_id, KEY_SIZE, &data_block, MY_MAX_FILE_SIZE);
	if (rc != UNQLITE_OK)
	{
		write_log("myfs_write - EIO");
		return -EIO;
	}

	// Update the fcb in-memory.
	the_root_fcb.size = written;
	time_t now = time(NULL);
	the_root_fcb.mtime = now;
	the_root_fcb.ctime = now;

	// Write the fcb to the store.
	rc = unqlite_kv_store(pDb, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE, &the_root_fcb, sizeof(myfcb));
	if (rc != UNQLITE_OK)
	{
		write_log("myfs_write - EIO");
		return -EIO;
	}

	return written;
}

// Set the size of a file.
// Read 'man 2 truncate'.
int myfs_truncate(const char *path, off_t newsize)
{
	write_log("myfs_truncate(path=\"%s\", newsize=%lld)\n", path, newsize);

	// Check that the size is acceptable
	if (newsize >= MY_MAX_FILE_SIZE)
	{
		write_log("myfs_truncate - EFBIG");
		return -EFBIG;
	}

	// Update the FCB in-memory
	the_root_fcb.size = newsize;

	// Write the fcb to the store.
	int rc = unqlite_kv_store(pDb, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE, &the_root_fcb, sizeof(myfcb));
	if (rc != UNQLITE_OK)
	{
		write_log("myfs_write - EIO");
		return -EIO;
	}

	return 0;
}

// Set permissions.
// Read 'man 2 chmod'.
int myfs_chmod(const char *path, mode_t mode)
{
	write_log("myfs_chmod(fpath=\"%s\", mode=0%03o)\n", path, mode);

	return 0;
}

// Set ownership.
// Read 'man 2 chown'.
int myfs_chown(const char *path, uid_t uid, gid_t gid)
{
	write_log("myfs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);

	return 0;
}

// Create a directory.
// Read 'man 2 mkdir'.
int myfs_mkdir(const char *path, mode_t mode)
{
	write_log("myfs_mkdir: %s\n", path);
	myfcb s_fcb = the_root_fcb;
	access_block s_dir;
	unqlite_int64 nBytes; //nBytes

	int rc; //Statement code
	int hier_lev = 0;
	char** hier_path = split_path(path, &hier_lev);

	//path1: /.../ hier_lev = 2, hier = 1, hier_path = [...]
	//path2: /.../.../ hier_lev = 3, hier = 2, hier_path = [..., ...]
	char* new_filename = hier_path[hier_lev - 2];
	
	if (hier_path) {
		for (int i = 0; i < hier_lev - 2; i++) {
			char *next = hier_path[i];
			rc = unqlite_kv_fetch(pDb, &(s_fcb.file_data_id), KEY_SIZE, &s_dir, &nBytes);
			if (rc != UNQLITE_OK) {
				error_handler(rc);
				write_log("mkdir: Root directory fetch failed: %i\n", rc);
				return -EIO;
			}
			int response;
			if ((response = readPath(next, s_dir, &s_fcb)) != 0) {
				return response;
			}
			if ((s_fcb.mode & S_IFDIR) != S_IFDIR) {
				write_log("mkdir: %s - This is not a directory\n", hier_path[i]);
				return -ENOENT;
			}
		}
	}
	else {
		write_log("mkdir: No enough memory space.\n");
		return -EIO;
	}
	free_strptr_arr(hier_path, hier_lev);
	
	//Now we are in the fcb of the last directory that may exists. Then we will read the dir of this fcb and find appropriate position for a new block
	uuid_t data_id;
	rc = find_free(s_fcb, &data_id);
	if (rc != 0) {
		return rc;
	}
	
	//Create the new directory
	
	

	return 0;
}

// Delete a file.
// Read 'man 2 unlink'.
int myfs_unlink(const char *path)
{
	write_log("myfs_unlink: %s\n", path);

	return 0;
}

// Delete a directory.
// Read 'man 2 rmdir'.
int myfs_rmdir(const char *path)
{
	write_log("myfs_rmdir: %s\n", path);

	return 0;
}

// OPTIONAL - included as an example
// Flush any cached data.
int myfs_flush(const char *path, struct fuse_file_info *fi)
{
	int retstat = 0;

	write_log("myfs_flush(path=\"%s\", fi=0x%08x)\n", path, fi);

	return retstat;
}

// OPTIONAL - included as an example
// Release the file. There will be one call to release for each call to open.
int myfs_release(const char *path, struct fuse_file_info *fi)
{
	int retstat = 0;

	write_log("myfs_release(path=\"%s\", fi=0x%08x)\n", path, fi);

	return retstat;
}

// OPTIONAL - included as an example
// Open a file. Open should check if the operation is permitted for the given flags (fi->flags).
// Read 'man 2 open'.
static int myfs_open(const char *path, struct fuse_file_info *fi)
{
	if (strcmp(path, the_root_fcb.path) != 0)
		return -ENOENT;

	write_log("myfs_open(path\"%s\", fi=0x%08x)\n", path, fi);

	//return -EACCES if the access is not permitted.

	return 0;
}

// This struct contains pointers to all the functions defined above
// It is used to pass the function pointers to fuse
// fuse will then execute the methods as required
static struct fuse_operations myfs_oper = {
	.getattr = myfs_getattr,
	.readdir = myfs_readdir,
	.open = myfs_open,
	.read = myfs_read,
	.create = myfs_create,
	.utime = myfs_utime,
	.write = myfs_write,
	.truncate = myfs_truncate,
	.flush = myfs_flush,
	.release = myfs_release,
};

void init_dir()
{
	printf("Hierarchy:\n");
	int rc;

	//root directory access block
	access_block root_dir;

	//Set the key value
	uuid_t *data_id = &(the_root_fcb.file_data_id);

	//Value for storing the data length
	unqlite_int64 nBytes;

	//Try to fetch root hierarchy element from the root_fcb
	rc = unqlite_kv_fetch(pDb, data_id, KEY_SIZE, &root_dir, &nBytes);

	//if it doesn't exist, we need to create it and writeback
	//This will be the hierarchy system for the root fcb
	if (rc == UNQLITE_NOTFOUND)
	{
		printf("Hierarchy: Not found - Creating hierarchy system:\n");

		//clear everything in the root_dir
		memset(&root_dir, 0, sizeof(access_block));

		//Write back
		printf("Writing to .db file:\n");
		rc = unqlite_kv_store(pDb, data_id, KEY_SIZE, &root_dir, sizeof(access_block));

		if (rc != UNQLITE_OK)
			error_handler(rc);
	}
	else
	{
		if (rc == UNQLITE_OK)
		{
			printf("init_store: root directory was found\n");
		}
		if (nBytes != sizeof(access_block))
		{
			printf("Data object has unexpected size %llu. Doing nothing.\n", nBytes);
			exit(-1);
		}
	}
}

// Initialise the in-memory data structures from the store. If the root object (from the store) is empty then create a root fcb (directory)
// and write it to the store. Note that this code is executed outide of fuse. If there is a failure then we have failed toi initlaise the
// file system so exit with an error code.
void init_fs()
{
	int rc;
	printf("init_fs\n");
	//Initialise the store.

	uuid_clear(zero_uuid);

	// Open the database.
	rc = unqlite_open(&pDb, DATABASE_NAME, UNQLITE_OPEN_CREATE);
	if (rc != UNQLITE_OK)
		error_handler(rc);

	unqlite_int64 nBytes; // Data length

	// Try to fetch the root element
	// The last parameter is a pointer to a variable which will hold the number of bytes actually read
	rc = unqlite_kv_fetch(pDb, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE, &the_root_fcb, &nBytes);

	// if it doesn't exist, we need to create one and put it into the database. This will be the root
	// directory of our filesystem i.e. "/"
	if (rc == UNQLITE_NOTFOUND)
	{

		printf("init_store: root object was not found\n");

		// clear everything in the_root_fcb
		memset(&the_root_fcb, 0, sizeof(myfcb));

		// Sensible initialisation for the root FCB
		//See 'man 2 stat' and 'man 2 chmod'.
		the_root_fcb.mode |= S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;
		the_root_fcb.mtime = time(0);
		the_root_fcb.uid = getuid();
		the_root_fcb.gid = getgid();
		uuid_generate(the_root_fcb.file_data_id);

		// Write the root FCB
		printf("init_fs: writing root fcb\n");
		rc = unqlite_kv_store(pDb, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE, &the_root_fcb, sizeof(myfcb));

		if (rc != UNQLITE_OK)
			error_handler(rc);
	}
	else
	{
		if (rc == UNQLITE_OK)
		{
			printf("init_store: root object was found\n");
		}
		if (nBytes != sizeof(myfcb))
		{
			printf("Data object has unexpected size. Doing nothing.\n");
			exit(-1);
		}
	}
}

void shutdown_fs()
{
	unqlite_close(pDb);
}

int main(int argc, char *argv[])
{
	int fuserc;
	struct myfs_state *myfs_internal_state;

	//Setup the log file and store the FILE* in the private data object for the file system.
	myfs_internal_state = malloc(sizeof(struct myfs_state));
	myfs_internal_state->logfile = init_log_file();

	//Initialise the file system. This is being done outside of fuse for ease of debugging.
	init_fs();
	init_dir();

	// Now pass our function pointers over to FUSE, so they can be called whenever someone
	// tries to interact with our filesystem. The internal state contains a file handle
	// for the logging mechanism
	fuserc = fuse_main(argc, argv, &myfs_oper, myfs_internal_state);

	//Shutdown the file system.
	shutdown_fs();

	return fuserc;
}
