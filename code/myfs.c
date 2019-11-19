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
// myent the_root_ent;
unqlite_int64 root_object_size_value = sizeof(myfcb);

// This is the pointer to the database we will use to store all our files
unqlite *pDb;
uuid_t zero_uuid;


//functions on save and read
int fetch_ent(uuid_t *key, myent *ent) {
	int rc;
	unqlite_int64 nBytes = sizeof(myent);

	rc = unqlite_kv_fetch(pDb, key, KEY_SIZE, NULL, &nBytes);
	if (nBytes != sizeof(myent)) {
		write_log("fetch_ent failed: invalid fetch size - %i, want: %i\n", nBytes, sizeof(myent));
		return rc;
	}
	rc = unqlite_kv_fetch(pDb, key, KEY_SIZE, ent, &nBytes);
	if (rc != UNQLITE_OK) {
		write_log("fetch_ent failed: error code - %i\n", rc);
		return rc;
	}
	return 0;
}

int fetch_fcb(uuid_t *key, myfcb *fcb) {
	int rc;
	unqlite_int64 nBytes = sizeof(myfcb);
	
	rc = unqlite_kv_fetch(pDb, key, KEY_SIZE, NULL, &nBytes);
	if (nBytes != sizeof(myfcb)) {
		write_log("fetch_ent failed: invalid fetch size - %i, want: %i\n", nBytes, sizeof(myfcb));
		return rc;
	}
	rc = unqlite_kv_fetch(pDb, key, KEY_SIZE, fcb, &nBytes);
	if (rc != UNQLITE_OK) {
		write_log("fetch_ent failed: error code - %i\n", rc);
		return rc;
	}
	return 0;
}

int fetch_file(uuid_t *key, myfile *file) {
	int rc; 
	unqlite_int64 nBytes = sizeof(myfile);

	rc = unqlite_kv_fetch(pDb, key, KEY_SIZE, NULL, &nBytes);
	if (nBytes != sizeof(myfile)) {
		write_log("fetch_file failed: invalid fetch size - %i, want: %i\n", nBytes, sizeof(myfile));
		return rc;
	}
	rc = unqlite_kv_fetch(pDb, key, KEY_SIZE, file, &nBytes);
	if (rc != UNQLITE_OK) {
		write_log("fetch_file failed: error code - %i\n", rc);
		return rc;
	}
	return 0;
}

int store_file(uuid_t *key, myfile *file) {
	int rc;
	rc = unqlite_kv_store(pDb, key, KEY_SIZE, file, sizeof(myfile));
	if (rc != UNQLITE_OK) {
		write_log("Store entrance failed\n");
		return rc;
	}
	return 0;
}

int store_ent(uuid_t *key, myent *ent) {
	int rc;
	rc = unqlite_kv_store(pDb, key, KEY_SIZE, ent, sizeof(myent));
	if (rc != UNQLITE_OK) {
		write_log("store_failed: Store entrance failed\n");
		return rc;
	}
	return 0;
}

int store_fcb(uuid_t *key, myfcb *fcb) {
	int rc;
	rc = unqlite_kv_store(pDb, key, KEY_SIZE, fcb, sizeof(myfcb));
	if (rc != UNQLITE_OK) {
		write_log("Store entrance failed\n");
		return rc;
	}
	return 0;
}


//functions on delete. Key is the key of the entrance
int deletion(uuid_t *key) {
	int rc;
	myent ent;
	myfcb fcb;

	//fetch fcb and entrance node
	if ((rc = fetch_ent(key, &ent)) != 0) {
		write_log("deletion: fetch entrance failed with %i\n", rc);
		return rc;
	}

	if ((rc = fetch_fcb(&(ent.fcb_id), &fcb)) != 0) {
		write_log("deletion: fetch fcb failed with %i\n", rc);
		return rc;
	}

	//delete all files
	for (int i = 0; i < MY_MAX_DIRECT; i++) {
		if (uuid_compare(fcb.direct[i], zero_uuid) != 0) {
			if ((rc = unqlite_kv_delete(pDb, &(fcb.direct[i]), KEY_SIZE)) != 0) {
				write_log("deletion: delete file failed.");
				return rc;
			}
		}
	}

	//delete fcb
	if ((rc = unqlite_kv_delete(pDb, &(ent.fcb_id), KEY_SIZE)) != 0) {
		write_log("deletion: delete entrance failed.");
		return rc;
	}

	//delete entry
	if ((rc = unqlite_kv_delete(pDb, key, KEY_SIZE)) != 0) {
		write_log("deletion: delete entrance failed.\n");
		return rc;
	}

	return 0;
}

//Functions on entrance finding
int find_entrance_with_name(char* path, myfcb *fcb, myent *ent) {
	int rc;
	for (int i = 0;i < MY_MAX_DIRECT; i++) {
		if (uuid_compare(zero_uuid,fcb -> direct[i]) != 0) {
			if((rc = fetch_ent(&(fcb->direct[i]), ent)) != 0) {
				write_log("find_path_with_name: Fetch_ent_failed. %i\n", rc);
				return rc;
			}
			if (strcmp(path, ent->name) == 0) {
				if((rc = fetch_fcb(&(ent->fcb_id), fcb)) != 0) {
					write_log("find_entrance_with_name: Fetch_fcb_failed. %i\n", rc);
					return rc;
				}
				return 0;
			}
		}
	}
	return -ENOENT;
}

//Find entrance does works for find the entrance of fcb required and return the fcb and the entrance node
int find_entrance(const char *path, myfcb* fcb, myent *ent) {
	char* s_path = strdup(path); 		//Copy path itself to prevent interrupt const value
	char* token = strtok(s_path, "/");  //Divide the path into tokens
	*fcb = the_root_fcb;
	int rc;

	while (token != NULL) {
		if ((rc = find_entrance_with_name(token, fcb, ent))!=0) {
			write_log("find_entrance: error with code %i\n", rc);
			return rc;
		}
		// write_log("find_ent: %s - expect %s\n", ent->name, token);
		token = strtok(0, "/");
	}
	return 0;
}

//functions on creative
int get_path_filename(const char *path, char ** file, char ** directory) {
	char *pathdup = strdup(path);
	char *last = strrchr(pathdup, '/');
	*last = '\0';
	*file = last + 1;
	if (last == pathdup) {
		*directory = "/";
	}
	else {
		*directory = pathdup;  //<= changed
	}
	return 0;
}

//This will create a fcb and a ent and generate a new uuid. Write back should be done outside the function
int create_fcb_with_ent(mode_t mode, char* name, myfcb *newFCB, myent* newENT) {
	memset(newFCB, 0, sizeof(myfcb));			//Memory init
	memset(newENT, 0, sizeof(myent));			//Memory init

	//Set the information of new fcb
	struct fuse_context *context = fuse_get_context();
	newFCB->uid = context -> uid;
	newFCB->gid = context -> gid;
	newFCB->mode = mode; 
	newFCB->size = sizeof(myfcb);

	//setup entrance
	strcpy(newENT->name, name);
	uuid_generate(newENT->fcb_id);
	return 0;
}

//This will generate a uuid in a free fcb access space with the entrance.
int free_space_generator(uuid_t* uuid, myent* ent) {
	myfcb fcb;
	int rc;
	if ((rc = fetch_fcb(&(ent->fcb_id), &fcb)) != 0) {
		write_log("Failed to generate a new free space key.");
		return rc;
	}
	for (int i = 0; i < MY_MAX_DIRECT; i++) {
		if (uuid_compare(zero_uuid, fcb.direct[i]) == 0) {
			uuid_generate(fcb.direct[i]);
			uuid_copy(*uuid, fcb.direct[i]);
			break;
		}
	}
	//write back fcb
	if ((rc = store_fcb(&(ent->fcb_id), &fcb)) != 0) {
		write_log("store_fcb: failed with err code %i\n", rc);
		return rc;
	}
	//fetch testing
	if ((rc = fetch_fcb(&(ent->fcb_id), NULL)) != 0) {
		write_log("store_fcb: failed to fetch with error code %i", rc);
		return rc;
	}
	return 0;
}

//This only works on the root fcb
int root_free_space_gen(uuid_t *uuid) {
	for (int i = 0; i < MY_MAX_DIRECT; i++) {
		if (uuid_compare(zero_uuid, the_root_fcb.direct[i]) == 0) {
			uuid_generate(the_root_fcb.direct[i]);
			uuid_copy(*uuid, the_root_fcb.direct[i]);
			break;
		}
	}
	int rc;
	//write back the root fcb
	if((rc = unqlite_kv_store(pDb,ROOT_OBJECT_KEY,ROOT_OBJECT_KEY_SIZE,&the_root_fcb,sizeof(myfcb))) != 0) {
		write_log("root_free_space_gen: Root FCB write_back failed %i", rc);
		return rc;
	}
	return 0;
}

//find the path, unlink the entrance
//add to free list(Extension)
int remove_node(char* filepath, char* filename) {
	int rc;
	//Following two will be used for storing path fcb and ent
	myfcb fcb;
	myent ent;

	//If it is in the root dir
	if (strcmp("/", filepath) == 0) {
		for (int i = 0; i < MY_MAX_DIRECT; i++) {
			if (uuid_compare(the_root_fcb.direct[i], zero_uuid) != 0) {
				if ((rc = fetch_ent(&(the_root_fcb.direct[i]), &ent)) != 0) {
					write_log("remove_node: fetch_ent failed with %i\n", rc);
					return rc;
				}
				if (strcmp(ent.name, filename) == 0) {
					deletion(&(the_root_fcb.direct[i]));
					uuid_clear(the_root_fcb.direct[i]);
					if((rc = unqlite_kv_store(pDb,ROOT_OBJECT_KEY,ROOT_OBJECT_KEY_SIZE,&the_root_fcb,sizeof(myfcb))) != 0) {
						write_log("root_free_space_gen: Root FCB write_back failed %i", rc);
						return rc;
					}
					return 0;
				}
			}
		}
		return -ENOENT;
	}

	fcb = the_root_fcb;
	if ((rc = find_entrance(filepath, &fcb, &ent)) != 0) {
		write_log("remove_node: find_entrance failed: %i", rc);
		return rc;
	}

	myent tmp;
	for (int i = 0; i < MY_MAX_DIRECT; i++) {
		if (uuid_compare(fcb.direct[i], zero_uuid) != 0) {
			if ((rc = fetch_ent(&(fcb.direct[i]), &tmp)) != 0) {
				write_log("remove_node: fetch_ent failed with %i\n", rc);
				return rc;
			}
			if (strcmp(ent.name, filename) == 0) {
				deletion(&(fcb.direct[i]));
				uuid_clear(fcb.direct[i]);
				if ((rc = store_fcb(&(ent.fcb_id), &fcb)) != 0) {
					write_log("free_spce: write_back failed with %i", rc);
					return rc;
				}
				return 0;
			}
		}
	}
	return -ENOENT;
}

//create all path does not exist
int create_new(char* path, char* name, mode_t mode) {
	int rc; 

	myfcb fcb;
	myent ent;
	uuid_t key;
	if (strcmp (path, "/") == 0) {
		if ((rc = root_free_space_gen(&key)) != 0) {
			write_log("create_dir - root_free_space_gen failed with error: %i", rc);
			return rc;
		}
	}
	else {
		fcb = the_root_fcb;
		int rc;
		if ((rc = find_entrance(path, &fcb, &ent)) != 0) {
			write_log("create_directory - find_entrance failed with %i\n", rc);
			return rc;
		}
		if ((rc = free_space_generator(&key, &ent)) != 0) {
			write_log("create_directory - space_generator_failed with %i\n", rc);
		}
	}
	if ((rc = create_fcb_with_ent(mode, name, &fcb, &ent)) != 0) {
		write_log("create_dir - Create Directory with error: %i", rc);
		return rc;
	}
	if ((rc = store_fcb(&(ent.fcb_id), &fcb)) != 0) {
		write_log("create_dir - Store fcb failed: %i\n", rc);
		return rc;
	}
	if ((rc = store_ent(&key, &ent)) != 0) {
		write_log("create_dir - store ent failed: %i", rc);
		return rc;
	}

	//Test fetch
	if ((rc = fetch_ent(&key, NULL)) != 0) {
		write_log("create_dir - fetch ent failed: %i", rc);
		return rc;
	}
	if ((rc = fetch_fcb(&(ent.fcb_id), NULL)) != 0) {
		write_log("create_dir - fetch fcb failed: %i", rc);
		return rc;
	}

	return 0;
}

//This will create an empty file
int create_file(uuid_t* key, myfile* file) {
	int rc;
	memset(file, 0, sizeof(myfile));

	//initialise
	if ((rc = store_file(key, file)) != 0) {
		write_log("create_file: new file create failed. error: %i\n", rc);
		return rc;
	}
	return 0;
}

// The functions which follow are handler functions for various things a filesystem needs to do:
// reading, getting attributes, truncating, etc. They will be called by FUSE whenever it needs
// your filesystem to do something, so this is where functionality goes.

// Get file and directory attributes (meta-data).
// Read 'man 2 stat' and 'man 2 chmod'.
static int myfs_getattr(const char *path, struct stat *stbuf) {

	write_log("myfs_getattr(path=\"%s\", statbuf=0x%08x)\n", path, stbuf);
	myfcb myfcb;
	myent myent;

	memset(stbuf, 0, sizeof(struct stat));

	if(strcmp(path, "/") ==0){
		myfcb = the_root_fcb;
	}else{
		int rc;
		if ((rc = find_entrance(path, &myfcb, &myent)) != 0) {
			write_log("myfs_getattr: failed with %i\n", rc);
			return rc;
		}
	}
	
	stbuf -> st_gid = myfcb.gid;
	stbuf -> st_mode = myfcb.mode;
	stbuf -> st_nlink = myfcb.nlink;
	stbuf -> st_mtime = myfcb.mtime;
	stbuf -> st_ctime = myfcb.ctime;
	stbuf -> st_size = myfcb.size;
	stbuf -> st_uid = myfcb.uid;

	return 0;
}

// Read a directory.
// Read 'man 2 readdir'.
static int myfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {

    (void) offset;  // This prevents compiler warnings
	(void) fi;

	write_log("write_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n", path, buf, filler, offset, fi);

    // We always output . and .. first, by convention. See documentation for more info on filler()
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	myfcb fcb;
	myent ent;
	int rc;
	// write_log("pointer - %s\n", path);

	if (strcmp("/", path) == 0) {
		fcb = the_root_fcb;
	}
	else{
		if ((rc = find_entrance(path, &fcb, &ent)) != 0) {
			write_log("readdir(): read the directory failed in find_path\n");
			return rc;
		}
	}

	for (int i = 0; i < MY_MAX_DIRECT; i++) {
		if (uuid_compare(zero_uuid, fcb.direct[i]) != 0) {
			if ((rc = fetch_ent(&(fcb.direct[i]), &ent)) != 0) {
				write_log("readdir(): fetch failed.\n");
				return rc;
			}
			else
			{
				filler(buf, ent.name, NULL, 0);
			}
		}
	}
	return 0;
}

// Read a file.
// Read 'man 2 read'.
static int myfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
	size_t len;
	(void) fi;
	
	write_log("myfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n", path, buf, size, offset, fi);
	
	myfcb ptrfcb;
	myent ptrent;
	myfile file;

	char* directory;
	char* fileName;
	int rc;
	get_path_filename(path, &fileName, &directory);
	if ((rc = find_entrance(path, &ptrfcb, &ptrent)) != 0) {
		write_log("myfs_read: Find entrance failed.\n");
		return rc;
	}
	
	if ((rc = fetch_file(&(ptrfcb.direct[0]), &file)) != 0) {
		memset(&file, 0, sizeof(myfile)); //If there is no file block, create one.
	}
	
	len = file.size;

	if (offset < len) {
		if (offset + size > len)
			size = len - offset;
		memcpy(buf, &(file.data) + offset, size);
	} else
		size = 0;
	
	return size;
}

// This file system only supports one file. Create should fail if a file has been created. Path must be '/<something>'.
// Read 'man 2 creat'.
static int myfs_create(const char *path, mode_t mode, struct fuse_file_info *fi){   
    write_log("myfs_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n", path, mode, fi);
	char* file;
	char* dir;
    get_path_filename(path, &file, &dir);
	
	int pathlen = strlen(file);
	if(pathlen>=MY_MAX_PATH){
		write_log("myfs_create - ENAMETOOLONG\n");
		return -ENAMETOOLONG;
	}

	return create_new(dir, file, mode | S_IFREG);
}

// Set update the times (actime, modtime) for a file. This FS only supports modtime.
// Read 'man 2 utime'.
static int myfs_utime(const char *path, struct utimbuf *ubuf){
    write_log("myfs_utime(path=\"%s\", ubuf=0x%08x)\n", path, ubuf);

	int rc;
	if(strcmp(path, "/") == 0) {
		the_root_fcb.mtime=ubuf->modtime;
		rc = unqlite_kv_store(pDb,ROOT_OBJECT_KEY,ROOT_OBJECT_KEY_SIZE,&the_root_fcb,sizeof(myfcb));
		if( rc != UNQLITE_OK ){
			write_log("myfs_write - EIO");
			return -EIO;
		}
	}
	else{
		myfcb fcb;
		myent ent;
		if ((rc = find_entrance(path, &fcb, &ent)) != 0) {
			write_log("myfs_utime: Error for %i\n", rc);
			return rc;
		}
		fcb.mtime = ubuf->modtime;
		if ((rc = store_fcb(&(ent.fcb_id), &fcb)) != 0) {
			write_log("myfs_mtime: Error for %i\n", rc);
			return rc;
		}
	}
    return 0;
}

// Write to a file.
// Read 'man 2 write'
static int myfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi){   
    write_log("myfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n", path, buf, size, offset, fi);
    
	if(size >= MY_MAX_FILE_SIZE){
		write_log("myfs_write - EFBIG");
		return -EFBIG;
	}
	int rc;
	char* filename;
	char* pathname;
	if ((rc = get_path_filename(path, &filename, &pathname)) != 0) {
		write_log("myfs_write - get_path_file_name failed\n", rc);
		return 0;
	}
	
	//create a new file with filename
	myfcb newfcb;
	myent newent;
	mode_t mode = S_IFREG|S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH;
	if ((rc = create_new(pathname, filename, mode)) != 0) {
		write_log("myfs_write: create_new failed with %i\n", rc);
		return rc;
	}

	if ((rc = find_entrance(path, &newfcb, &newent)) != 0) {
		write_log("myfs_write: find_entrance failed with %i\n", rc);
		return rc;
	}	//now the fcb is the fcb of the new file

	uuid_t key;
	if ((rc = free_space_generator(&key, &newent)) != 0) {
		write_log("myfs_write: find_free_space failed with %i\n", rc);
		return rc;
	}	//Now we get a key for storing the file block
	
	myfile newFile;
	if ((rc = create_file(&key, &newFile)) != 0) {
		write_log("myfs_write: create file failed with rc = %i\n", rc);
	}

	// Write the data in-memory and into data struct
    int written = snprintf(newFile.data, MY_MAX_FILE_SIZE, buf);
	newFile.size = written;

	// Write the data block back to the store.
	if ((rc = store_file(&key, &newFile)) != 0) {
		write_log("Store file failed with error %i.\n", rc);
		return rc;
	}

	// Update the fcb in-memory.
	newfcb.size=written;
	time_t now = time(NULL);
	newfcb.mtime=now;
	newfcb.ctime=now;

	// Write the fcb to the store.
    // Write the data block to the store.
	if ((rc = store_fcb(&(newent.fcb_id), &newfcb)) != 0) {
		write_log("Store file failed.\n");
		return rc;
	}
	
    return written;
}

// Set the size of a file.
// Read 'man 2 truncate'.
int myfs_truncate(const char *path, off_t newsize){    
    write_log("myfs_truncate(path=\"%s\", newsize=%lld)\n", path, newsize);
    
    // Check that the size is acceptable
	if(newsize >= MY_MAX_FILE_SIZE){
		write_log("myfs_truncate - EFBIG");
		return -EFBIG;
	}
	
    myfcb fcb;
	myent ent;
	int rc;
	if((rc = find_entrance(path, &fcb, &ent)) != 0) {
		write_log("truncate: Error wil code %i", rc);
	}
	myfile oldfile;
	myfile newfile;
	memset(&newfile, 0, sizeof(newfile));
	if ((rc = fetch_file(&fcb.direct[0], &oldfile))!=0) {
		write_log("truncate: Error wil code %i", rc);
		return rc;
	}
	memcpy(&newfile, &oldfile, oldfile.size);
	newfile.size = newsize;
	fcb.mtime = time(NULL);

	if ((rc = store_file(&fcb.direct[0], &newfile)) != 0) {
		write_log("truncate: Error with code %i", rc);
		return rc;
	}
	if ((rc = store_fcb(&(ent.fcb_id), &fcb)) != 0) {
		write_log("truncate: Error with code %i", rc);
	}
	return 0;
}

// Set permissions.
// Read 'man 2 chmod'.
int myfs_chmod(const char *path, mode_t mode){
    write_log("myfs_chmod(fpath=\"%s\", mode=0%03o)\n", path, mode);
    myent ent;
	myfcb fcb;
	int rc;
	if (strcmp(path, "/") == 0) {
		return -EIO;
	}
	if ((rc = find_entrance(path, &fcb, &ent)) != 0) {
		write_log("chmod: Permission change failed.\n");
		return rc;
	}
	if ((fcb.mode & S_IFDIR) == S_IFDIR) {
		fcb.mode = mode | S_IFDIR;
	}
	else
	{
		fcb.mode = mode | S_IFREG;
	}
	if((rc = store_fcb(&(ent.fcb_id), &fcb)) != 0) {
		write_log("Chmod: permission change failed\n");
		return rc;
	}
    return 0;
}

// Set ownership.
// Read 'man 2 chown'.
int myfs_chown(const char *path, uid_t uid, gid_t gid){   
    write_log("myfs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);
   	myent ent;
	myfcb fcb;
	int rc;
	if (strcmp(path, "/") == 0) {
		return -EIO;
	}
	if ((rc = find_entrance(path, &fcb, &ent)) != 0) {
		write_log("chmod: Permission change failed.\n");
		return rc;
	}
	fcb.uid = uid;
	fcb.gid = gid;
	if((rc = store_fcb(&(ent.fcb_id), &fcb)) != 0) {
		write_log("Chmod: permission change failed\n");
		return rc;
	}
    return 0;
}

// Create a directory.
// Read 'man 2 mkdir'.
int myfs_mkdir(const char *path, mode_t mode){
	write_log("myfs_mkdir: %s\n",path);	
	
	char* file;
	char* dir;
    get_path_filename(path, &file, &dir);

	int pathlen = strlen(file);
	if(pathlen>=MY_MAX_PATH){
		write_log("myfs_create - ENAMETOOLONG\n");
		return -ENAMETOOLONG;
	}

	return create_new(dir, file, mode | S_IFDIR);
}

// Delete a file.
// Read 'man 2 unlink'.
int myfs_unlink(const char *path){
	write_log("myfs_unlink: %s\n",path);	
	char* filepath;
	char* filename;
	get_path_filename(path, &filename, &filepath);
    return remove_node(filepath, filename);
}

// Delete a directory.
// Read 'man 2 rmdir'.
int myfs_rmdir(const char *path){
    write_log("myfs_rmdir: %s\n",path);	
	char* filepath;
	char* filename;
	get_path_filename(path, &filename, &filepath);
    return remove_node(filepath, filename);
}

// OPTIONAL - included as an example
// Flush any cached data.
int myfs_flush(const char *path, struct fuse_file_info *fi){
    int retstat = 0;
    
    write_log("myfs_flush(path=\"%s\", fi=0x%08x)\n", path, fi);
	
    return retstat;
}

// OPTIONAL - included as an example
// Release the file. There will be one call to release for each call to open.
int myfs_release(const char *path, struct fuse_file_info *fi){
    int retstat = 0;
    
    write_log("myfs_release(path=\"%s\", fi=0x%08x)\n", path, fi);
    
    return retstat;
}

// OPTIONAL - included as an example
// Open a file. Open should check if the operation is permitted for the given flags (fi->flags).
// Read 'man 2 open'.
static int myfs_open(const char *path, struct fuse_file_info *fi){
	// if (strcmp(path, the_root_fcb.path) != 0)
	// 	return -ENOENT;
		
	write_log("myfs_open(path\"%s\", fi=0x%08x)\n", path, fi);
	
	//return -EACCES if the access is not permitted.

	return 0;
}

// This struct contains pointers to all the functions defined above
// It is used to pass the function pointers to fuse
// fuse will then execute the methods as required 
static struct fuse_operations myfs_oper = {
	.getattr	= myfs_getattr,
	.readdir	= myfs_readdir,
	.open		= myfs_open,
	.read		= myfs_read,
	.create		= myfs_create,
	.utime 		= myfs_utime,
	.write		= myfs_write,
	.truncate	= myfs_truncate,
	.flush		= myfs_flush,
	.release	= myfs_release,
	.mkdir 		= myfs_mkdir,
	.chmod  	= myfs_chmod,
	.chown 		= myfs_chown,
	.unlink 	= myfs_unlink,
	.rmdir		= myfs_rmdir,
};


// Initialise the in-memory data structures from the store. If the root object (from the store) is empty then create a root fcb (directory)
// and write it to the store. Note that this code is executed outide of fuse. If there is a failure then we have failed toi initlaise the 
// file system so exit with an error code.
void init_fs(){
	int rc;
	printf("init_fs\n");
	//Initialise the store.
    
	uuid_clear(zero_uuid);
	// Open the database.
	rc = unqlite_open(&pDb,DATABASE_NAME,UNQLITE_OPEN_CREATE);
	if( rc != UNQLITE_OK ) error_handler(rc);

	unqlite_int64 nBytes;  // Data length

	// Try to fetch the root element
    // The last parameter is a pointer to a variable which will hold the number of bytes actually read
	rc = unqlite_kv_fetch(pDb,ROOT_OBJECT_KEY,ROOT_OBJECT_KEY_SIZE,&the_root_fcb,&nBytes);

    // if it doesn't exist, we need to create one and put it into the database. This will be the root
    // directory of our filesystem i.e. "/"
	if(rc==UNQLITE_NOTFOUND) {      

		printf("init_store: root object was not found\n");

        // clear everything in the_root_fcb
		memset(&the_root_fcb, 0, sizeof(myfcb));
				
        // Sensible initialisation for the root FCB
		//See 'man 2 stat' and 'man 2 chmod'.
		the_root_fcb.mode |= S_IFDIR|S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH; 
		the_root_fcb.mtime = time(NULL);
		the_root_fcb.ctime = time(NULL);
		the_root_fcb.uid = getuid();
		the_root_fcb.gid = getgid();
		the_root_fcb.nlink = 2;
		the_root_fcb.nlink = sizeof(myfcb);
		
        // Write the root FCB
		printf("init_fs: writing root fcb\n");
		rc = unqlite_kv_store(pDb,ROOT_OBJECT_KEY,ROOT_OBJECT_KEY_SIZE,&the_root_fcb,sizeof(myfcb));
        if( rc != UNQLITE_OK ) error_handler(rc);
    } 
    else
    {
     	if(rc==UNQLITE_OK) { 
	 		printf("init_store: root object was found\n"); 
        }
	 	if(nBytes!=sizeof(myfcb)) { 
			printf("Data object has unexpected size. Doing nothing.\n");
			exit(-1);
        }
    }
}

void shutdown_fs(){
	unqlite_close(pDb);
}

int main(int argc, char *argv[]){	
	int fuserc;
	struct myfs_state *myfs_internal_state;

	//Setup the log file and store the FILE* in the private data object for the file system.	
	myfs_internal_state = malloc(sizeof(struct myfs_state));
    myfs_internal_state->logfile = init_log_file();
	
	//Initialise the file system. This is being done outside of fuse for ease of debugging.
	init_fs();
		
    // Now pass our function pointers over to FUSE, so they can be called whenever someone
    // tries to interact with our filesystem. The internal state contains a file handle
    // for the logging mechanism
	fuserc = fuse_main(argc, argv, &myfs_oper, myfs_internal_state);
	
	//Shutdown the file system.
	shutdown_fs();
	
	return fuserc;
}

