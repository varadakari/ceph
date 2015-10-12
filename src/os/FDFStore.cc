// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/types.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "FDFStore.h"
#include "lz4.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "fdfstore(" << path << ") "

#define COLL_ATTR_PREFIX "user.ceph."
#define OMAP_HEADER_SUFFIX "$_omap_header"

#define MAX_NUM_CNTRS 8192

// for write combining per-thread globals
#define MAX_BATCH 20
#define MAX_KEY_SZ 250
static __thread ZS_cguid_t p_cguid = (ZS_cguid_t)0;
static __thread ZS_obj_t p_obj[MAX_BATCH];
static __thread uint32_t p_num_obj = 0;
static __thread char p_prefix[MAX_KEY_SZ] = "\0";
//static __thread char prepend[256];

void FDFStore::fdf_write_to_mput_wrapper(ZS_cguid_t cguid, char* key, uint32_t key_len,
                                            char *data, uint64_t data_len)
{
  if(p_num_obj){
  if(p_cguid != cguid) _mput();
  else if(strcmp(key, p_prefix) < 0 ) _mput();
  else if(p_num_obj == MAX_BATCH) _mput();
  }

  /* Write the object */
  assert(p_num_obj < MAX_BATCH);
  p_obj[p_num_obj].flags = 0;
  p_obj[p_num_obj].key_len = key_len;
  p_obj[p_num_obj].data_len = data_len;
  p_obj[p_num_obj].key =  (char *) malloc(p_obj[p_num_obj].key_len);
  memcpy(p_obj[p_num_obj].key, key, p_obj[p_num_obj].key_len);
  p_obj[p_num_obj].data =  (char *)malloc(p_obj[p_num_obj].data_len);
  memcpy(p_obj[p_num_obj].data, data, p_obj[p_num_obj].data_len);
  p_cguid = cguid;
  strncpy(p_prefix, p_obj[p_num_obj].key, p_obj[p_num_obj].key_len);
  p_prefix[p_obj[p_num_obj].key_len] = '\0';
  dout(0) << __func__ << " cguid: " << p_cguid << "key: " << p_obj[p_num_obj].key 
  << " datalen: " << p_obj[p_num_obj].data_len << dendl;
  p_num_obj++;
  assert(p_num_obj <= MAX_BATCH);
}

// for comparing collections for lock ordering
bool operator>(const FDFStore::CollectionRef& l,
	       const FDFStore::CollectionRef& r)
{
  return (unsigned long)l.get() > (unsigned long)r.get();
}

const string FDFStore::DATA_SEPARATOR("\1\1");
const string FDFStore::ATTR_SEPARATOR("\1\2");
const string FDFStore::OMAP_SEPARATOR("\1\3");
const string FDFStore::COLL_ATTR_SEPARATOR("\1\4");
const string FDFStore::END_SEPARATOR("\1\5");

/* 
 * ghobject_t to be saved as fdf key, to be retrieved later
 *
 * ghobject_t
 *   hobject_t
 *     object_t 
 *       name
 *     snapid_t
 *     hash
 *     pool
 *     nspace
 *   gen_t
 *   shard_t
 *
 * fdf key will have following format
 * name/snap/hash/pool/nspace/[generation/shard_id/]*optional
 */

int FDFStore::ghobject_to_string(const ghobject_t& oid, string& key)
{
  char tmp[256];

  //name
  key = oid.hobj.oid.name + "/";

  //snap
  if (oid.hobj.snap == CEPH_NOSNAP)
    key += "head";
  else if (oid.hobj.snap == CEPH_SNAPDIR)
    key += "snapdir";
  else{
    snprintf(tmp, 255, "%llx", (long long unsigned)oid.hobj.snap);
    key += tmp;
  }
  key += "/";

  //hash
  snprintf(tmp, 255, "%u", oid.hobj.get_hash());
  key += tmp;
  key += "/";

  //pool
  snprintf(tmp, 255, "%ld", oid.hobj.pool);
  key += tmp;
  key += "/";

  //nspace
  key += oid.hobj.nspace + "/";

  //generation TBD
  //shard_id TBD
  return 0;
}

int FDFStore::string_to_ghobject( char *str, ghobject_t *oid)
{
  char *tok = str;
  hobject_t ho;
  int i = 0;
  int s = strlen(str);

  //name
  while ((tok[i] != '/') && (i < s)) i++;
  str[i] = '\0';
  if (tok[0] == '/') ho.oid.name = string("");
  else ho.oid.name = string(tok);
  i++;
  tok = &str[i];

  //snap
  while ((str[i] != '/') && (i < s)) i++;
  str[i] = '\0';
  if (strcmp(tok, "head") == 0)
    ho.snap = CEPH_NOSNAP;
  else if (strcmp(tok, "snapdir") == 0)
    ho.snap = CEPH_SNAPDIR;
  else
    ho.snap = strtoull(tok, NULL, 16);
  i++;
  tok = &str[i];

  //hash
  while ((str[i] != '/') && (i < s)) i++;
  str[i] = '\0';
  if (tok[0] == '/') ho.set_hash(atol(""));
  else ho.set_hash(atoi(tok));
  i++;
  tok = &str[i];

  //pool
  while ((str[i] != '/') && (i < s)) i++;
  str[i] = '\0';
  if (tok[0] == '/') ho.pool = atoll("");
  else ho.pool = atol(tok);
  i++;
  tok = &str[i];

  //nspace
  while ((str[i] != '/') && (i < s)) i++;
  str[i] = '\0';
  if (tok[0] == '/') ho.nspace = string("");
  else ho.nspace = string(tok);

  //generation TBD
  //shard_t TBD
  *oid = ghobject_t(hobject_t(ho));
  return 0;
}

//support for tessellation and compression
__thread float ccr = 1; //current compression ratio
static uint64_t mcsz = 64*1024; //max chunk size, 64KB
//static uint64_t mcsz = 65300;
//static uint64_t mcsz = 32500;
//static uint64_t mcsz = 16150;
static int maxosz = 16; //max offset size, 16
static const char *maxoffset = "9999999999999999";
static const char *minoffset = "0000000000000000";
#define SHIM_TESSELLATION_ENABLE // tessellation enabler
#define SHIM_COMPRESSION_ENABLE // compression enabler

/*
 * "exists" will be set as follows,
 * 0 - object doesnt exist
 * 1 - object exists
 * "tess" will be set as follows,
 * 0 - normal object
 * 1 - tessellated object
 * "datalen" will be set to max object size
 */
ZS_status_t FDFStore::fdf_object_exists(
                        struct ZS_thread_state *ts,
                        ZS_cguid_t cguid,
                        char *key,
                        uint32_t keylen,
                        int *exists,
                        int *tess,
                        uint64_t *datalen)
{
    ZS_status_t rc = ZS_SUCCESS;
    if(exists) *exists = 0;
    if(tess) *tess = 0;
    if(datalen) *datalen = 0;

    ZS_range_meta_t meta;
    memset(&meta, 0, sizeof(ZS_range_meta_t));
    struct ZS_cursor *crusor;

    meta.flags = (ZS_range_enums_t)(ZS_RANGE_START_LE | ZS_RANGE_END_GE | ZS_RANGE_KEYS_ONLY);
    meta.key_start = (char *) calloc((keylen+(2*maxosz)), sizeof(char));
    memcpy(meta.key_start, key, keylen);
    memcpy(meta.key_start+keylen, maxoffset, maxosz);
    meta.keylen_start = keylen+(2*maxosz);
    meta.key_end = (char *) calloc((keylen+(2*maxosz)), sizeof(char));
    memcpy(meta.key_end, key, keylen);
    memcpy(meta.key_end+keylen, minoffset, maxosz);
    meta.keylen_end = keylen+(2*maxosz);
    dout(0) << __func__ << " Start key: " << meta.key_start << " Start keylen: "<< meta.keylen_start << " End key: " << meta.key_end << " End keylen: "<< meta.keylen_end << dendl;

    rc = ZSGetRange(ts, cguid, ZS_RANGE_PRIMARY_INDEX, &crusor, &meta);
    assert(rc == ZS_SUCCESS);

    ZS_range_data_t val[1];
    memset(val, 0 , sizeof(ZS_range_data_t));
    int out = 0;

    rc = ZSGetNextRange(ts, crusor, 1, &out, val);
    if(out == 1){
	assert(rc == ZS_SUCCESS);
	if(exists) *exists = 1;
	if(tess) *tess = 1;
	if(datalen){
	    char *chk = (char *) malloc((maxosz+1)*sizeof(char));
	    memset(chk, 0, maxosz+1);
	    memcpy(chk, (val[0].key)+keylen+maxosz, maxosz);
	    assert(strncmp(chk, minoffset, maxosz) >= 0);
	    assert(strncmp(chk, maxoffset, maxosz) <= 0);
	    *datalen = atoll(chk)+1;
	    free(chk);
	}
	ZSFreeBuffer(val[0].key);
    }

    ZSGetRangeFinish(ts, crusor);
    free(meta.key_start);
    free(meta.key_end);

    return rc;
}

/*
 * Given a tessellated object/offset/len, will provide list of
 * objects which have the tessellates.
 * "fdfov" will have array of object names
 * and "ovcnt" will have number of objects it spans
 * caller need to free the buffers in any case.
 */
ZS_status_t FDFStore::fdf_object_list(
                        struct ZS_thread_state *ts,
                        ZS_cguid_t cguid,
                        char *key,
                        uint32_t keylen,
                        uint64_t offset,
                        int len,
                        char ***fdfov,
                        int *ovcnt)
{
    ZS_status_t rc = ZS_SUCCESS;
    int ocnt = 0;
    int sd = 0;
    int ed = 0;
    //zero length objects possible ??
    if(len == 0){
	*ovcnt = ocnt;
	return rc;
    }

    int curr_ov_sz = 1;
    int ov_sz_mul = 10;
    char **ov = (char **)malloc(curr_ov_sz * sizeof(char *));

    ZS_range_meta_t meta;
    memset(&meta, 0, sizeof(ZS_range_meta_t));
    struct ZS_cursor *crusor;
    ZS_range_data_t val[20];
    memset(val, 0 , 20*sizeof(ZS_range_data_t));
    int out;

	char so[maxosz+1],eo[maxosz+1];
	snprintf(so, maxosz+1, "%016llu", offset);
	snprintf(eo, maxosz+1, "%016llu", offset+len-1);
	
	meta.flags = (ZS_range_enums_t)(ZS_RANGE_START_LT | ZS_RANGE_END_GE | ZS_RANGE_KEYS_ONLY);
	meta.key_start = (char *) calloc((keylen+(2*maxosz)), sizeof(char));
	memcpy(meta.key_start, key, keylen);
	memcpy(meta.key_start+keylen, so, maxosz);
	meta.keylen_start = keylen+(2*maxosz);
	meta.key_end = (char *) calloc((keylen+(2*maxosz)), sizeof(char));
	memcpy(meta.key_end, key, keylen);
   	memcpy(meta.key_end+keylen, minoffset, maxosz);
	meta.keylen_end =keylen+(2*maxosz);
	dout(0) << __func__ << " Start key: " << meta.key_start << " Start keylen: "<< meta.keylen_start << " End key: " << meta.key_end << " End keylen: "<< meta.keylen_end << dendl;

	rc = ZSGetRange(ts, cguid, ZS_RANGE_PRIMARY_INDEX, &crusor, &meta);
	assert(rc == ZS_SUCCESS);

	rc = ZSGetNextRange(ts, crusor, 1, &out, val);
	if(out == 1){
		uint64_t teoff=0;
		char teoffa[maxosz+1];
		memset(teoffa, 0, maxosz+1);
		strncpy(teoffa, val[0].key+keylen+maxosz, maxosz);
		teoffa[maxosz] = '\0';
		teoff = atoll(teoffa);
		dout(0) << __func__ << " teoffa: " << teoffa << " teoff: " << teoff << dendl;

		if(offset <= teoff){
			ocnt++;
			if(ocnt > curr_ov_sz){
				curr_ov_sz *= ov_sz_mul;
				ov = (char **)realloc(ov, curr_ov_sz * sizeof(char *));
			}
			ov[ocnt - 1] = (char *) calloc((keylen+(2*maxosz)), sizeof(char));
			memcpy(ov[ocnt - 1], val[0].key, keylen+(2*maxosz));
			ZSFreeBuffer(val[0].key);
		}
	}

	rc = ZSGetRangeFinish(ts, crusor);
	free(meta.key_start);
	free(meta.key_end);

	memset(&meta, 0, sizeof(ZS_range_meta_t));
	memset(val, 0 , 20*sizeof(ZS_range_data_t));
	out = 0;
	meta.flags = (ZS_range_enums_t)(ZS_RANGE_START_GE | ZS_RANGE_END_LE | ZS_RANGE_KEYS_ONLY);
	meta.key_start = (char *) calloc((keylen+(2*maxosz)), sizeof(char));
        memcpy(meta.key_start, key, keylen);
        memcpy(meta.key_start+keylen, so, maxosz);
        meta.keylen_start = keylen+(2*maxosz);
        meta.key_end = (char *) calloc((keylen+(2*maxosz)), sizeof(char));
        memcpy(meta.key_end, key, keylen);
        memcpy(meta.key_end+keylen, eo, maxosz);
        meta.keylen_end =keylen+(2*maxosz);
	dout(0) << __func__ << " Start key: " << meta.key_start << " Start keylen: "<< meta.keylen_start << " End key: " << meta.key_end << " End keylen: "<< meta.keylen_end << dendl;

        rc = ZSGetRange(ts, cguid, ZS_RANGE_PRIMARY_INDEX, &crusor, &meta);
        assert(rc == ZS_SUCCESS);

	do{
		rc = ZSGetNextRange(ts, crusor, 20, &out, val);
		int i;
		for(i=0;i<out;i++){
			ocnt++;
			if(ocnt > curr_ov_sz){
                                curr_ov_sz *= ov_sz_mul;
                                ov = (char **)realloc(ov, curr_ov_sz * sizeof(char *));
                        }
                        ov[ocnt - 1] = (char *) calloc((keylen+(2*maxosz)), sizeof(char));
                        memcpy(ov[ocnt - 1], val[i].key, keylen+(2*maxosz));
                        ZSFreeBuffer(val[i].key);
		}
	} while(out == 20);

	rc = ZSGetRangeFinish(ts, crusor);
        free(meta.key_start);
        free(meta.key_end);
	
		
    *fdfov = ov;
    *ovcnt = ocnt;
    return rc;
}

ZS_status_t FDFStore::fdf_object_range(
                        struct ZS_thread_state *ts,
                        ZS_cguid_t cguid,
                        char *key,
                        uint32_t keylen,
                        uint64_t offset,
                        uint64_t len,
                        char **fdfov,
                        int fdfovcnt,
                        uint64_t *so,
                        uint64_t *eo)
{
    ZS_status_t rc = ZS_SUCCESS;

    if(so) *so = offset;
    if(eo) *eo = offset+len-1;
    char tso[maxosz+1];
    char te[maxosz+1];

    if (fdfovcnt == 0){
	return rc;
    } else {
	memset(tso, 0, maxosz+1);
	memset(te, 0, maxosz+1);
	memcpy(tso, fdfov[0]+keylen, maxosz);
	memcpy(te, fdfov[fdfovcnt-1]+keylen+maxosz, maxosz);
	if((uint64_t)atoll(tso) < offset){
	    if(so) *so = atoll(tso);
	}
	if((uint64_t)atoll(te) > offset+len-1){
	    if(eo) *eo = atoll(te);
	}
    }
    return rc;
}

/*
 * will provide data in buffer of desired len from offset.
 * in such case "data" will have required data buffer.
 * caller need to free the buffer.
 */
ZS_status_t FDFStore::fdf_object_join(
                        struct ZS_thread_state *ts,
                        ZS_cguid_t cguid,
                        char *key,
                        uint32_t keylen,
                        uint64_t offset,
                        uint64_t len,
                        char **data,
                        char **fdfov,
                        int fdfovcnt)
{

    ZS_status_t rc = ZS_SUCCESS;

	assert(fdfovcnt > 0);
	ZS_range_meta_t meta;
	memset(&meta, 0, sizeof(ZS_range_meta_t));
	struct ZS_cursor *crusor;
	meta.flags = (ZS_range_enums_t)(ZS_RANGE_START_GE | ZS_RANGE_END_LE);
	meta.key_start = (char *) calloc((keylen+(2*maxosz)), sizeof(char));
	memcpy(meta.key_start, fdfov[0], keylen+(2*maxosz));
	meta.keylen_start = keylen+(2*maxosz);
	meta.key_end = (char *) calloc((keylen+(2*maxosz)), sizeof(char));
	memcpy(meta.key_end, fdfov[fdfovcnt-1], keylen+(2*maxosz));
	meta.keylen_end = keylen+(2*maxosz);
	dout(0) << __func__ << "Start key: " << meta.key_start << "Start keylen: "<< meta.keylen_start << " End key: " << meta.key_end << " End keylen: "<< meta.keylen_end << dendl;

	rc = ZSGetRange(ts, cguid, ZS_RANGE_PRIMARY_INDEX, &crusor, &meta);
	assert(rc == ZS_SUCCESS);

	ZS_range_data_t *val = (ZS_range_data_t *)calloc(fdfovcnt, sizeof(ZS_range_data_t));
	int out = 0;

	rc = ZSGetNextRange(ts, crusor, fdfovcnt, &out, val);
	assert(out == fdfovcnt);

    int i;
    uint64_t tsoff=0;
    uint64_t teoff=0;
    char tsoffa[maxosz+1];
    char teoffa[maxosz+1];
    char *tmpdata = (char *) calloc(len, sizeof(char));
    if(data) *data = tmpdata;

    for(i=0;i<out;i++){
	memset(tsoffa, 0, maxosz+1);
	strncpy(tsoffa, val[i].key+keylen, maxosz);
	tsoffa[maxosz] = '\0';
	tsoff = atoll(tsoffa);
	memset(teoffa, 0, maxosz+1);
	strncpy(teoffa, val[i].key+keylen+maxosz, maxosz);
	teoffa[maxosz] = '\0';
	teoff = atoll(teoffa);
#ifndef SHIM_COMPRESSION_ENABLE
	if((tsoff >= offset) && ((offset+len-1) >= teoff)){
	    memcpy(tmpdata+(tsoff - offset), val[i].data, (teoff - tsoff + 1));
	} else if((tsoff <= offset) && ((offset+len-1) <= teoff)){
	    memcpy(tmpdata, val[i].data+(offset - tsoff), len);
	} else if((tsoff >= offset) && ((offset+len-1) <= teoff)){
	    memcpy(tmpdata+(tsoff - offset), val[i].data, (offset+len-tsoff));
	} else if((tsoff <= offset) && ((offset+len-1) >= teoff)){
	    memcpy(tmpdata, val[i].data+(offset - tsoff), teoff - offset + 1);
	} else assert(0==1);
#else //SHIM_COMPRESSION_ENABLE
	int dclen =  teoff - tsoff + 1;
	uint64_t tdatalen = val[i].datalen;
	char *uc = (char *) calloc(dclen, sizeof(char));
	int ucsz = LZ4_decompress_safe(val[i].data, uc, tdatalen, dclen);
	assert(0 != ucsz);
	assert(ucsz == dclen);
	if((tsoff >= offset) && ((offset+len-1) >= teoff)){
	    memcpy(tmpdata+(tsoff - offset), uc, (teoff - tsoff + 1));
	} else if((tsoff <= offset) && ((offset+len-1) <= teoff)){
	    memcpy(tmpdata, uc+(offset - tsoff), len);
	} else if((tsoff >= offset) && ((offset+len-1) <= teoff)){
	    memcpy(tmpdata+(tsoff - offset), uc, (offset+len-tsoff));
	} else if((tsoff <= offset) && ((offset+len-1) >= teoff)){
	    memcpy(tmpdata, uc+(offset - tsoff), teoff - offset + 1);
	} else assert(0==1);
	free(uc);
#endif //SHIM_COMPRESSION_ENABLE
	ZSFreeBuffer(val[i].key);
	ZSFreeBuffer(val[i].data);
    }
	rc = ZSGetRangeFinish(ts, crusor);
        free(meta.key_start);
        free(meta.key_end);
	free(val);
    return rc;
}

/*
 * Given dynamic string array and count, free space.
 */
void FDFStore::fdf_object_free_list(char **fdfov, int fdfovcnt)
{
    int i=0;
    for(;i < fdfovcnt; i++){
	free(fdfov[i]);
    }
}

/*
 * Given list of objects and count, delete those fdf object.
 */
ZS_status_t FDFStore::fdf_object_delete_list(
                        struct ZS_thread_state *ts,
                        ZS_cguid_t cguid,
                        char *key,
                        uint32_t keylen,
                        char **fdfov,
                        int fdfovcnt)
{
    ZS_status_t rc = ZS_SUCCESS;
    int i = 0;
    for(;i < fdfovcnt; i++){
	rc = ZSDeleteObject( ts, cguid, fdfov[i], keylen+(2*maxosz));
	assert(ZS_SUCCESS == rc);
    }
    return rc;
}

/*
 * Given object/offset/len tessellates to multiple fdf objects.
 */
ZS_status_t FDFStore::fdf_object_split(
                        struct ZS_thread_state *ts,
                        ZS_cguid_t cguid,
                        char *key,
                        uint32_t keylen,
                        char *data,
                        uint64_t datalen,
                        uint64_t offset)
{
    ZS_status_t rc = ZS_SUCCESS;
    uint64_t i = 0;
    char *ton = (char *) calloc((keylen+(2*maxosz)+1), sizeof(char));
#ifndef SHIM_COMPRESSION_ENABLE
    uint64_t wsz;
    for(i = offset; datalen > 0; i+=wsz, datalen-=wsz){
	wsz = (datalen > mcsz) ? mcsz : datalen;
	memset(ton, 0, keylen+(2*maxosz)+1);
	memcpy(ton, key, keylen);
	snprintf(ton+keylen, (2*maxosz)+1, "%016lu%016lu", i, i+wsz-1);
//tomy wc
fdf_write_to_mput_wrapper(cguid, ton, keylen+(2*maxosz), &data[i-offset], wsz);
/*
	rc = ZSWriteObject( ts, cguid, ton,
		keylen+(2*maxosz), &data[i-offset], wsz, 0);
	assert(ZS_SUCCESS == rc);
*/
    }
#else //SHIM_COMPRESSION_ENABLE
    //uint64_t indatalen = datalen;
    //uint64_t outdatalen = 0;
    //uint64_t cratio = 0;

    uint64_t bound = LZ4_compressBound(mcsz);
    char *out = (char *) calloc(bound, sizeof(char));
    assert(out != NULL);

    uint64_t ilen = (datalen < mcsz) ? datalen : mcsz;
    uint64_t olen = LZ4_compress(data, out, ilen);
    assert(olen != 0);
    ccr = floorf(100 * ((float)ilen / (float)olen)) / 100;

    uint64_t iclen;
    for(i = offset; datalen > 0; i+=iclen, datalen-=iclen){
	iclen = floor(ccr * mcsz);
	iclen = (datalen < iclen) ? datalen : iclen;
	memset(out, 0, bound);
	olen = LZ4_compress( &data[i-offset], out, iclen);
	assert(olen != 0);
	ccr = floorf(100 * ((float)iclen / (float)olen)) / 100;
	memset(ton, 0, keylen+(2*maxosz)+1);
	memcpy(ton, key, keylen);
	snprintf(ton+keylen, (2*maxosz)+1, "%016lu%016lu", i, i+iclen-1);
	//outdatalen += olen;
//tomy wc
fdf_write_to_mput_wrapper(cguid, ton, keylen+(2*maxosz), out, olen);
/*
	rc = ZSWriteObject( ts, cguid, ton,
		keylen+(2*maxosz), out, olen, 0);
	assert(ZS_SUCCESS == rc);
*/
    }
    free(out);
    //cratio = floorf(100 * ((float)indatalen / (float)outdatalen)) / 100;
    //dout(0) << "fdf_tnc " << indatalen << " " << outdatalen << " " << cratio << dendl;
#endif //SHIM_COMPRESSION_ENABLE
    free(ton);
    return rc;
}
  	
struct ZS_thread_state *FDFStore::fdf_get_thd_state()
{
  ZS_status_t rc;
  struct ZS_thread_state *current = fdf_thd_state.get();
  if (current != NULL) {
    return current;
  }

  rc = ZSInitPerThreadState(fdf_state, &current);
  if (rc != ZS_SUCCESS) {
    derr << "FDFInitPerThreadState failed: " << ZSStrError(rc) << dendl;
    return NULL;
  }
  
  fdf_thd_state.reset(current);
  return current;
}

FDFStore::CollectionRef FDFStore::fdf_get_container_for_coll(coll_t cid)
{
  CollectionRef c = get_collection(cid);
  if (!c) {
    assert(fdf_get_thd_state());
    ZS_cguid_t cguid;
    ZS_status_t rc;
    /* Open container */
    ZS_container_props_t props;
    ZSLoadCntrPropDefaults(&props);
    props.flash_only = ZS_TRUE;
	props.durability_level = ZS_DURABILITY_HW_CRASH_SAFE;

    dout(1) << "Collection: "<< cid.to_str() << " is not associated with"
            << " fdf container yet. open one" << dendl;
    rc = ZSOpenContainer(fdf_get_thd_state(), (char *)cid.c_str(),
                          &props, ZS_CTNR_RW_MODE, &cguid);
    if(rc != ZS_SUCCESS) {
      dout(10) << "FDF Open collection(" << cid.c_str() << ") failed : " << ZSStrError(rc) << dendl;
      return c;
    }
    RWLock::WLocker l(coll_lock);
    c.reset(new Collection);
    c->cguid = cguid;
    coll_map[cid] = c;
  }
  return c;
}

int FDFStore::peek_journal_fsid(uuid_d *fsid)
{
  *fsid = uuid_d();
  return 0;
}

int FDFStore::mount()
{
  if (!init_done) {
    dout(1) << "FDFInit in " << __func__ << dendl;
    ZSSetProperty("ZS_REFORMAT",  "0");
    assert(ZSInit(&fdf_state) == ZS_SUCCESS);
    init_done = true;
    int r = _load();
    if (r < 0)
      return r;
  }
  finisher.start();
  return 0;
}

int FDFStore::umount()
{
  finisher.stop();
  return _save();
}

int FDFStore::_save()
{
#if 0
  assert(fdf_get_thd_state());
  RWLock::RLocker l(coll_lock); // block any writer
  for (hash_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    dout(10) << __func__ << " coll " << p->first << " " << p->second << dendl;
    ZSFlushContainer(fdf_get_thd_state(), p->second->cguid);
    ZSCloseContainer(fdf_get_thd_state(), p->second->cguid);
  }
#endif

  return 0;
}

#if 0
void FDFStore::dump_all()
{
  Formatter *f = new_formatter("json-pretty");
  f->open_object_section("store");
  dump(f);
  f->close_section();
  dout(0) << "dump:";
  f->flush(*_dout);
  *_dout << dendl;
  delete f;
}
#endif

void FDFStore::dump(Formatter *f)
{
  f->open_array_section("collections");
  for (hash_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    f->open_object_section("collection");
    f->dump_string("name", stringify(p->first));

    f->close_section();
  }
  f->close_section();
}

int FDFStore::_load()
{
  ZS_status_t rc;
  ZS_cguid_t cguid, fdf_cids[MAX_NUM_CNTRS];
  uint32_t n_cguids, i;
  ZS_container_props_t props;
  char name[64] = "meta";
  assert(fdf_get_thd_state());
  /*
  rc = ZSGetContainers(fdf_get_thd_state(), fdf_cids, &n_cguids);
  if( rc != ZS_SUCCESS ) {
    derr << "Unable to get FDF container list: " << ZSStrError(rc) << dendl;
    return -1;
  }
  for ( i = 0; i < n_cguids; i++ ) {
    rc = ZSGetContainerProps(fdf_get_thd_state(),fdf_cids[i],&props);
    if( rc != ZS_SUCCESS ) {
      derr << "Unable to get container props for " << fdf_cids[i]
              << ", " << ZSStrError(rc) << dendl;
      return -1;
    }
dout(0) << "tomy: name: " << props.name << " flash_only: " << props.flash_only << dendl;
    rc = ZSOpenContainer(fdf_get_thd_state(), props.name,
                          &props, ZS_CTNR_RW_MODE, &cguid);
    if(rc != ZS_SUCCESS) {
      dout(10) << __func__ << " FDF Open collection(" 
              << props.name << ") failed : " << ZSStrError(rc) << dendl;
      return -1;
    }
    dout(1) << __func__ << " collection(" 
            << props.name << ", " << cguid << ") successfully" << dendl;
    CollectionRef c(new Collection(cguid));
    coll_t coll(props.name);
    RWLock::WLocker l(coll_lock);
    coll_map.insert(hash_map<coll_t, CollectionRef>::value_type(coll, c));
  }
  */
  //tomy hack
  ZSLoadCntrPropDefaults(&props);
  props.flash_only = ZS_TRUE;
	props.durability_level = ZS_DURABILITY_HW_CRASH_SAFE;
  strncpy(props.name, name, 64);

  rc = ZSOpenContainer(fdf_get_thd_state(), props.name,
                        &props, ZS_CTNR_RW_MODE, &cguid);
  if(rc != ZS_SUCCESS) {
       dout(0) << "tomy: meta open failed." << dendl;
       return -1;
  }
  CollectionRef c(new Collection(cguid));
  coll_t coll(props.name);
  RWLock::WLocker l(coll_lock);
  coll_map.insert(hash_map<coll_t, CollectionRef>::value_type(coll, c));
  //tomy hack end

  return 0;  
}

void FDFStore::set_fsid(uuid_d u)
{
  int r = write_meta("fs_fsid", stringify(u));
  assert(r >= 0);
}

uuid_d FDFStore::get_fsid()
{
  string fsid_str;
  int r = read_meta("fs_fsid", &fsid_str);
  assert(r >= 0);
  uuid_d uuid;
  bool b = uuid.parse(fsid_str.c_str());
  assert(b);
  return uuid;
}

int FDFStore::mkfs()
{
  string fsid_str;
  int r = read_meta("fs_fsid", &fsid_str);
  if (r == -ENOENT) {
    uuid_d fsid;
    fsid.generate_random();
    fsid_str = stringify(fsid);
    r = write_meta("fs_fsid", fsid_str);
    if (r < 0)
      return r;
    dout(1) << __func__ << " new fsid " << fsid_str << dendl;
  } else {
    dout(1) << __func__ << " had fsid " << fsid_str << dendl;
  }

  // ceph first do a mkfs, which call FDFStore::mkfs()
  // and FDFStore::mount() sequentially.
  // ceph then start normally and call FDFStore::mount()
  // need handle the FDF_REFORMAT properly
  dout(1) << "FDFInit in " << __func__ << dendl;
  ZSSetProperty("ZS_REFORMAT",  "1");
  assert(ZSInit(&fdf_state) == ZS_SUCCESS);
  init_done = true;

  return 0;
}

int FDFStore::statfs(struct statfs *st)
{
  dout(10) << __func__ << dendl;
  // make some shit up.  these are the only fields that matter.
  st->f_bsize = 1024;
  st->f_blocks = 1000000;
  st->f_bfree =  1000000;
  st->f_bavail = 1000000;
  return 0;
}

objectstore_perf_stat_t FDFStore::get_cur_stats()
{
  // fixme
  return objectstore_perf_stat_t();
}

FDFStore::CollectionRef FDFStore::get_collection(coll_t cid)
{
  RWLock::RLocker l(coll_lock);
  hash_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}


// ---------------
// read operations

bool FDFStore::exists(coll_t cid, const ghobject_t& oid)
{
    dout(10) << __func__ << " " << cid << " " << oid << dendl;
    CollectionRef c = get_collection(cid);
    if (!c)
	return false;

    string key;
    ghobject_to_string(oid, key);
    key = key + DATA_SEPARATOR;
#ifndef SHIM_TESSELLATION_ENABLE
    ZS_status_t rc;
    char *data = NULL;
    uint64_t datalen = 0;

    rc = ZSReadObject(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
	    key.length(), &data, &datalen);
    if( rc != ZS_SUCCESS ) {
	dout(10) << "FDF container(" << cid.to_str() << "), cguid("
	    << c->cguid << ") " << __func__ << " failed(key:"
	    << oid << "), " << ZSStrError(rc) << dendl;
	return false;
    }
    ZSFreeBuffer(data);
    return true;
#else //SHIM_TESSELLATION_ENABLE
    int exists = 0;
    fdf_object_exists(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
	    key.length(), &exists, 0, 0);
    //assert((FDF_SUCCESS|FDF_QUERY_DONE) == rc);
    if(exists) return true;
    return false;
#endif //SHIM_TESSELLATION_ENABLE
}

int FDFStore::stat(
    coll_t cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;

  /* Get the FDF container associated with the collection */
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  ZS_status_t rc;
  string key;
  ghobject_to_string(oid, key);
  key = key + DATA_SEPARATOR;

  char *data = NULL;
  uint64_t datalen;

  rc = ZSReadObject(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
      key.length(), &data, &datalen);
  if (rc == ZS_OBJECT_UNKNOWN) {
    return -ENOENT;
  } else if (rc != ZS_SUCCESS) {
     dout(10) << "FDF container(" << cid.to_str() << "), cguid("
          << c->cguid << ") " << __func__ << " failed(key:"
          << oid << "), " << ZSStrError(rc) << dendl;
    return -1;
  }

  ZSFreeBuffer(data);

  st->st_size = datalen;
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
  st->st_nlink = 1;

  return 0;
}

int FDFStore::read(
    coll_t cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags,
    bool allow_eio)
{
    dout(10) << __func__ << " " << cid << " " << oid << " "
	<< offset << "~" << len << dendl;

    ZS_status_t rc;
    string key;
    ghobject_to_string(oid, key);
    key = key + DATA_SEPARATOR;

    /* Get the FDF container associated with the collection */
    CollectionRef c = fdf_get_container_for_coll(cid);
    if (!c)
	return -ENOENT;
#ifndef SHIM_TESSELLATION_ENABLE
    char *data = NULL;
    uint64_t datalen = 0;
    /* read the object */
    rc = ZSReadObject(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
	    key.length(), &data, &datalen);
    if(rc == ZS_OBJECT_UNKNOWN) return -ENOENT;
    assert(rc == ZS_SUCCESS);
    if(offset > datalen) return 0;

    size_t l = len;
    if(len == 0){
	l = datalen;
	bufferptr bp = buffer::create(l);
	memcpy(bp.c_str(), data, l);
	bl.push_back(bp);
    } else if(offset+len < datalen) {
	bufferptr bp = buffer::create(l);
	memcpy(bp.c_str(), data+offset, l);
	bl.push_back(bp);
    } else {
	l = datalen - offset;
	bufferptr bp = buffer::create(l);
	memcpy(bp.c_str(), data+offset, l);
	bl.push_back(bp);
    }
    if(data) ZSFreeBuffer(data);

    return l;
#else //SHIM_TESSELLATION_ENABLE
    int exists = 0;
    int tess = 0;
    uint64_t datalen = 0;
    dout(0) << __func__ << " key: " << key <<dendl;

    rc = fdf_object_exists(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
	    key.length(), &exists, &tess, &datalen);
    //assert((FDF_SUCCESS|FDF_QUERY_DONE) == rc);

    if(exists == 0) {
	return -ENOENT;
    }

    assert(tess == 1);

    if(offset > datalen) return 0;

    uint64_t l = len;
    uint64_t o = offset;
    if(len == 0){
	o = 0;
	l = datalen;
    } else if(offset+len <= datalen) {
	l = len;
    } else {
	l = datalen - offset;
    }

    char **fdfov=NULL;
    int ovcnt=0;

    rc = fdf_object_list(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
	    key.length(), o, l, &fdfov, &ovcnt);
    //assert((FDF_SUCCESS|FDF_QUERY_DONE) == rc);

    char *data = NULL;
    rc = fdf_object_join(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
	    key.length(), o, l, &data, fdfov, ovcnt);
    assert(ZS_SUCCESS == rc);

    bufferptr bp = buffer::create(l);
    memcpy(bp.c_str(), data, l);
    bl.push_back(bp);
    if(ovcnt) fdf_object_free_list(fdfov, ovcnt);
    free(fdfov);
    free(data);
    return (size_t)l;
#endif //SHIM_TESSELLATION_ENABLE
    return 0;
}

int FDFStore::fiemap(coll_t cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, bufferlist& bl)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << offset << "~"
	   << len << dendl;

  /* Get the FDF container associated with the collection */
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  ZS_status_t rc;
  string key;
  ghobject_to_string(oid, key);
  key = key + DATA_SEPARATOR;

  char *data = NULL;
  uint64_t datalen;

  rc = ZSReadObject(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
        key.length(), &data, &datalen);
  if (rc == ZS_OBJECT_UNKNOWN) {
    return -ENOENT;
  } else if (rc != ZS_SUCCESS) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
          << c->cguid << ") " << __func__ << " failed(key:"
          << oid << ", offset: " << offset << ", len: " << len << "), " 
          << ZSStrError(rc) << dendl;
    return -1;
  }

  if (offset >= datalen){
	ZSFreeBuffer(data);//leak
    return 0;
  }

  size_t l = len;
  if (offset + l > datalen)
    l = datalen - offset;

  map<uint64_t, uint64_t> m;
  m[offset] = l;
  ::encode(m, bl);
  
  ZSFreeBuffer(data);//leak
  return 0;  
}

int FDFStore::getattr(coll_t cid, const ghobject_t& oid,
		      const char *name, bufferptr& value)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << name << dendl;
  ZS_status_t rc;

  /* Get the FDF container associated with the collection */
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  char *data = NULL;
  uint64_t datalen = 0;

  string key_attr;
  ghobject_to_string(oid, key_attr);
  key_attr = key_attr + ATTR_SEPARATOR + name;
  /* read the object */
  rc = ZSReadObject(fdf_get_thd_state(), c->cguid, (char *)key_attr.c_str(),
                      key_attr.length(), &data, &datalen);
  if (rc == ZS_OBJECT_UNKNOWN) {
    return -ENODATA;
  } else if( rc != ZS_SUCCESS ) {
     dout(10) << "FDF container(" << cid.to_str() << "), cguid("
          << c->cguid << ") " << __func__ << " failed(key:"
          << oid << ", attr: " << name << "), " << ZSStrError(rc) << dendl;
    return -1;
  }

  value = buffer::create(datalen);
  memcpy(value.c_str(), data, datalen);
  ZSFreeBuffer(data);

  return 0;
}

//int FDFStore::getattrs(coll_t cid, const ghobject_t& oid,
//		       map<string,bufferptr>& aset, bool user_only)
int FDFStore::getattrs(coll_t cid, const ghobject_t& oid,
			map<string,bufferptr>& aset)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;

  /* Get the FDF container associated with the collection */
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  string key;
  ghobject_to_string(oid, key);

  int attr_range_done;

  ZS_status_t rc;
  ZS_range_meta_t  rmeta;
  struct ZS_cursor *cursor;
  ZS_range_data_t  *values;
  int i, n_in, n_out;

  memset(&rmeta, 0, sizeof(rmeta));

  ZS_range_enums_t flags = static_cast<ZS_range_enums_t>
                              (ZS_RANGE_START_GT | ZS_RANGE_END_LT);
  rmeta.flags = flags;

  string key_start = key + ATTR_SEPARATOR;
  size_t encoded_len = key_start.length();

  /*  1 is the trailing '\0' */
  rmeta.key_start = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_start);
  memcpy(rmeta.key_start, key_start.c_str(), key_start.length());
  rmeta.keylen_start = encoded_len;

  string key_end = key + OMAP_SEPARATOR;
  encoded_len = key_end.length();

  /* 1 is the trailing '\0' */
  rmeta.key_end = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_end);
  memcpy(rmeta.key_end, key_end.c_str(), key_end.length());
  rmeta.keylen_end = encoded_len;
  
  /* Initiate the range */
  rc = ZSGetRange(fdf_get_thd_state(), c->cguid, 
                      ZS_RANGE_PRIMARY_INDEX, &cursor, &rmeta);
  if( rc != ZS_SUCCESS) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
            << c->cguid << ") " << __func__ << " failed for FDFGetRange: "
            << ZSStrError(rc) << dendl;
    aset.clear();
    free(rmeta.key_start);
    free(rmeta.key_end);
    return -1;
  }

  free(rmeta.key_start);
  free(rmeta.key_end);

  n_in = 1;
  values = (ZS_range_data_t *) malloc(sizeof(ZS_range_data_t) * n_in);
  assert(values); 
  memset(values, 0, sizeof(ZS_range_data_t) * n_in);
  /* Traverse the range */
  attr_range_done = 0;
  while(1) {
    rc = ZSGetNextRange(fdf_get_thd_state(), cursor, n_in, &n_out, values);
    if( rc == ZS_SUCCESS || rc == ZS_QUERY_DONE) {
      if (n_out == 0) {
        break;
      }

      for (i = 0; i < n_out; i++) {
         string key_attr(values[i].key, values[i].keylen);
         if(key_attr.substr(0, key.length()) == key) {
           /*Add it to the set */
           string attr = key_attr.substr(key.length() + 2);
/*           if (user_only) {
             if (attr.length() > 1 && attr[0] == '_') {
               aset.insert(std::make_pair(attr.substr(1), bufferptr(values[i].data, values[i].datalen)));
             }
           } else {*/
             aset.insert(std::make_pair(attr, bufferptr(values[i].data, values[i].datalen)));
//           }
         } else {
           //FIXME: a mismatch encoded key
         } 
         ZSFreeBuffer(values[i].key);
         ZSFreeBuffer(values[i].data);
      }
    } else {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
              << c->cguid << ") " << __func__ << " failed for FDFGetNextRange: "
              << ZSStrError(rc) << ", aset.size(): " << aset.size() << dendl;
      break;
    }
  }
  /* Finish the enumeration */
  rc = ZSGetRangeFinish(fdf_get_thd_state(), cursor);
  if ( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid(" << c->cguid
              << ") " << __func__ << " failed for FDFGetNextRangeFinish: "
              << ZSStrError(rc) << dendl;
  }

  /* Free allocated memory */
  free(values);
  return 0;
}

int FDFStore::list_collections(vector<coll_t>& ls)
{
  dout(10) << __func__ << dendl;
  RWLock::RLocker l(coll_lock);
  for (hash_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    ls.push_back(p->first);
  }
  return 0;
}

bool FDFStore::collection_exists(coll_t cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  RWLock::RLocker l(coll_lock);
  return coll_map.count(cid);
}

int FDFStore::collection_getattr(coll_t cid, const char *name,
				 void *value, size_t size)
{
  dout(10) << __func__ << " " << cid << " " << name << dendl;
  ZS_status_t rc;

  CollectionRef c = fdf_get_container_for_coll(cid);

  string coll_attr = COLL_ATTR_PREFIX + cid.to_str() + COLL_ATTR_SEPARATOR + name;

  char *data = NULL;
  uint64_t datalen = 0;
  rc = ZSReadObject(fdf_get_thd_state(), c->cguid,
                      (char *)coll_attr.c_str(), coll_attr.length(),
                      &data, &datalen);
  if( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
         << c->cguid << ") " << __func__ << " failed(key:" << name
         << "), " << ZSStrError(rc) << dendl;
    return -1;
  }

  size_t l = MIN(size, (size_t)datalen);
  memcpy(value, data, l);
  ZSFreeBuffer(data);
  return l;
}

int FDFStore::collection_getattr(coll_t cid, const char *name, bufferlist& bl)
{
  dout(10) << __func__ << " " << cid << " " << name << dendl;
  ZS_status_t rc;

  CollectionRef c = fdf_get_container_for_coll(cid);

  string coll_attr = cid.to_str() + name;

  char *data = NULL;
  uint64_t datalen = 0;
  rc = ZSReadObject(fdf_get_thd_state(), c->cguid,
                      (char *)coll_attr.c_str(), coll_attr.length(),
                      &data, &datalen);
  if( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
         << c->cguid << ") " << __func__ << " failed(key:" << name
         << "), " << ZSStrError(rc) << dendl;
    return -1;
  }

  bufferptr bp = buffer::create(datalen);
  memcpy(bp.c_str(), data, datalen);
  bl.push_back(bp);

  ZSFreeBuffer(data);
  return bl.length();
}

int FDFStore::collection_getattrs(coll_t cid, map<string,bufferptr> &aset)
{
  dout(10) << __func__ << " " << cid << dendl;
  /* Get the FDF container associated with the collection */
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  string key = COLL_ATTR_PREFIX + cid.to_str();

  int attr_range_done;

  ZS_status_t rc;
  ZS_range_meta_t  rmeta;
  struct ZS_cursor *cursor;
  ZS_range_data_t  *values;
  int i, n_in, n_out;

  memset(&rmeta, 0, sizeof(rmeta));

  ZS_range_enums_t flags = static_cast<ZS_range_enums_t>
                              (ZS_RANGE_START_GT | ZS_RANGE_END_LT);
  rmeta.flags = flags;

  string key_start = key + COLL_ATTR_SEPARATOR;
  size_t encoded_len = key_start.length();

  /*  1 is the trailing '\0' */
  rmeta.key_start = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_start);
  memcpy(rmeta.key_start, key_start.c_str(), key_start.length());
  rmeta.keylen_start = encoded_len;

  string key_end = key + END_SEPARATOR;
  encoded_len = key_end.length();

  /* 1 is the trailing '\0' */
  rmeta.key_end = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_end);
  memcpy(rmeta.key_end, key_end.c_str(), key_end.length());
  rmeta.keylen_end = encoded_len;
  
  /* Initiate the range */
  rc = ZSGetRange(fdf_get_thd_state(), c->cguid, 
                      ZS_RANGE_PRIMARY_INDEX, &cursor, &rmeta);
  if( rc != ZS_SUCCESS) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
            << c->cguid << ") " << __func__ << " failed for FDFGetRange: "
            << ZSStrError(rc) << dendl;
    aset.clear();
    free(rmeta.key_start);
    free(rmeta.key_end);
    return -1;
  }

  free(rmeta.key_start);
  free(rmeta.key_end);

  n_in = 1;
  values = (ZS_range_data_t *) malloc(sizeof(ZS_range_data_t) * n_in);
  assert(values); 
  memset(values, 0, sizeof(ZS_range_data_t) * n_in);
  /* Traverse the range */
  attr_range_done = 0;
  while(1) {
    rc = ZSGetNextRange(fdf_get_thd_state(), cursor, n_in, &n_out, values);
    if( rc == ZS_SUCCESS || rc == ZS_QUERY_DONE) {
      if (n_out == 0) {
        break;
      }

      for (i = 0; i < n_out; i++) {
        string key_attr(values[i].key, values[i].keylen);
        if(key_attr.substr(0, key.length()) == key) {
          /*Add it to the set */
          string attr = key_attr.substr(key.length() + 2);
          aset.insert(std::make_pair(attr, bufferptr(values[i].data, values[i].datalen)));
        } else {
          //FIXME: a mismatch encoded key
        } 
        ZSFreeBuffer(values[i].key);
        ZSFreeBuffer(values[i].data);
      }
    } else {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
              << c->cguid << ") " << __func__ << " failed for FDFGetNextRange: "
              << ZSStrError(rc) << ", aset.size(): " << aset.size() << dendl;
      break;
    }
  }
  /* Finish the enumeration */
  rc = ZSGetRangeFinish(fdf_get_thd_state(), cursor);
  if ( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid(" << c->cguid
              << ") " << __func__ << " failed for FDFGetNextRangeFinish: "
              << ZSStrError(rc) << dendl;
  }

  /* Free allocated memory */
  free(values);
  return 0;
}

bool FDFStore::collection_empty(coll_t cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  // call proper FDF API
  return false;
}

int FDFStore::collection_list(coll_t cid, vector<ghobject_t>& o)
{
  dout(10) << __func__ << " " << cid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  // call FDFEnumeration
  return 0;
}

int FDFStore::collection_list_partial(coll_t cid, ghobject_t start,
				      int min, int max, snapid_t snap, 
				      vector<ghobject_t> *ls, ghobject_t *next)
{
    dout(10) << __func__ << " " << cid << " " << start << " " << min << "-"
	<< max << " " << snap << dendl;

    CollectionRef c = fdf_get_container_for_coll(cid);
    if (!c)
	return -ENOENT;

    *next = start;
    string key;
    ghobject_to_string(start, key);
    ghobject_t oid;

    int attr_range_done = 0;
    char con[256], *ton;
    memset(con, 0, 256);
    strncpy(con, key.c_str(), key.length());

    ZS_status_t rc;
    ZS_range_meta_t  rmeta;
    struct ZS_cursor *cursor;
    ZS_range_data_t  *values;
    int i=0, n_in=0, n_out=0;

    memset(&rmeta, 0, sizeof(rmeta));

    ZS_range_enums_t flags = static_cast<ZS_range_enums_t>
	(ZS_RANGE_START_GE | ZS_RANGE_KEYS_ONLY);
    rmeta.flags = flags;

    string key_start = key + DATA_SEPARATOR;
    size_t encoded_len = key_start.length();

    rmeta.key_start = (char *)calloc(1, (encoded_len + 1));
    memcpy(rmeta.key_start, key_start.c_str(), key_start.length());
    rmeta.keylen_start = encoded_len;

    /* Initiate the range */
    rc = ZSGetRange(fdf_get_thd_state(), c->cguid,
	    ZS_RANGE_PRIMARY_INDEX, &cursor, &rmeta);
    if( rc != ZS_SUCCESS) {
	dout(10) << "FDF container(" << cid.to_str() << "), cguid("
	    << c->cguid << ") " << __func__ << " failed for FDFGetRange: "
	    << ZSStrError(rc) << dendl;
	ls->clear();
	free(rmeta.key_start);
	return -1;
    }

    free(rmeta.key_start);
    //  free(rmeta.key_end);

    n_in = 1;

    values = (ZS_range_data_t *) malloc(sizeof(ZS_range_data_t) * n_in);
    assert(values);
    memset(values, 0, sizeof(ZS_range_data_t) * n_in);
    /* Traverse the range */
    attr_range_done = 0;
    while(!attr_range_done) {
	n_out = 0;
	rc = ZSGetNextRange(fdf_get_thd_state(), cursor, n_in, &n_out, values);
	if( rc == ZS_FAILURE){
	    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
		<< c->cguid << ") " << __func__ << " failed for FDFGetNextRange: "
		<< ZSStrError(rc) << dendl;
	    ZSGetRangeFinish(fdf_get_thd_state(), cursor);
	    free(values);
	    return -1;
	}
	if( rc == ZS_QUERY_DONE){
	    dout(0) << "collection_list_partial: QUERY DONE !!" << dendl;
	    attr_range_done = 1;
	    *next = ghobject_t::get_max();
	}
	if( rc == ZS_SUCCESS) {
	    for (i = 0; i < n_out; i++){
		if((ton = strstr(values[i].key, DATA_SEPARATOR.c_str())) != NULL){
		    *ton = '\0';
		    uint32_t tl = strlen(values[i].key);
		    if((tl != key.length()) || strncmp(key.c_str(), values[i].key, key.length())){

			//ghobject_t oid;
			string_to_ghobject(values[i].key, &oid);
			if (ls->size() < (unsigned)max) {
			    ls->push_back(oid);
			} else {
			    *next = oid;
			    attr_range_done = 1;
			    break;
			}
		    }
		}
		ZSFreeBuffer(values[i].key);
		ZSFreeBuffer(values[i].data);
	    }
	}
    }
    /*
       for(;i > 0, i < n_out; i++){
       ZSFreeBuffer(values[i].key);
       ZSFreeBuffer(values[i].data);
       }
     */
    /* Finish the enumeration */
    rc = ZSGetRangeFinish(fdf_get_thd_state(), cursor);
    if ( rc != ZS_SUCCESS ) {
	dout(10) << "FDF container(" << cid.to_str() << "), cguid(" << c->cguid
	    << ") " << __func__ << " failed for FDFGetNextRangeFinish: "
	    << ZSStrError(rc) << dendl;
    }
    /* Free allocated memory */
    free(values);

    return 0;
}

int FDFStore::collection_list_range(coll_t cid,
				    ghobject_t start, ghobject_t end,
				    snapid_t seq, vector<ghobject_t> *ls)
{
  dout(10) << __func__ << " " << cid << " " << start << " " << end
	   << " " << seq << dendl;
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  string start_key, end_key;
  ghobject_to_string(start, start_key);
  ghobject_to_string(end, end_key);

  start_key = start_key + DATA_SEPARATOR;
  end_key = end_key + DATA_SEPARATOR;

  ZS_range_meta_t  rmeta;
  struct ZS_cursor *cursor;
  ZS_range_data_t  *values;
  int i, n_in, n_out;

  ZS_status_t rc;
  memset(&rmeta, 0, sizeof(rmeta));

  ZS_range_enums_t flags = static_cast<ZS_range_enums_t>
              (ZS_RANGE_START_GT | ZS_RANGE_END_LT | ZS_RANGE_KEYS_ONLY);
  rmeta.flags = flags;

  size_t encoded_start_len = start_key.length();
  /*  1 is the trailing '\0' */
  rmeta.key_start = (char *)malloc(encoded_start_len + 1);
  assert(rmeta.key_start);

  memset(rmeta.key_start, 0, encoded_start_len + 1);
  memcpy(rmeta.key_start, start_key.c_str(), start_key.length());
  rmeta.keylen_start = encoded_start_len;

  size_t encoded_end_len = end_key.length();
  /* 1 is the trailing '\0' */
  rmeta.key_end = (char *)malloc(encoded_end_len + 1);
  assert(rmeta.key_end);
  memset(rmeta.key_end, 0, encoded_end_len + 1);
  memcpy(rmeta.key_end, end_key.c_str(), end_key.length());
  rmeta.keylen_end = encoded_end_len;
  
  /* Initiate the range */
  rc = ZSGetRange(fdf_get_thd_state(), c->cguid, 
                      ZS_RANGE_PRIMARY_INDEX, &cursor, &rmeta);
  if( rc != ZS_SUCCESS) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
            << c->cguid << ") " << __func__ << " failed for FDFGetRange: "
            << ZSStrError(rc) << dendl;
    free(rmeta.key_start);
    free(rmeta.key_end);
    return -1;
  }

  free(rmeta.key_start);
  free(rmeta.key_end);

  n_in = 1;
  values = (ZS_range_data_t *) malloc(sizeof(ZS_range_data_t) * n_in);
  assert(values); 
  memset(values, 0, sizeof(ZS_range_data_t) * n_in);
  /* Traverse the range */

  ghobject_t oid;
  while(1) {
    rc =ZSGetNextRange(fdf_get_thd_state(), cursor, n_in, &n_out, values);
    if (rc == ZS_SUCCESS || rc == ZS_QUERY_DONE) {
      if (n_out == 0) {
        break;
      }

      for (i = 0; i < n_out; i++) {
        size_t sep_len = DATA_SEPARATOR.length();
        size_t sep_off_len = values[i].keylen - sep_len;
        if(memcmp(values[i].key + sep_off_len, DATA_SEPARATOR.c_str(), sep_len) == 0) {
          values[i].key[sep_off_len] = '\0';
          string_to_ghobject(values[i].key, &oid);
          ls->push_back(oid);

          ZSFreeBuffer(values[i].key);
        }
      }
    } else {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
              << c->cguid << ") " << __func__ << " failed for FDFGetNextRange: "
              << ZSStrError(rc) << dendl;
    }
  }
  /* Finish the enumeration */
  rc = ZSGetRangeFinish(fdf_get_thd_state(), cursor);
  if ( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid(" << c->cguid
              << ") " << __func__ << " failed for FDFGetNextRangeFinish: "
              << ZSStrError(rc) << dendl;
  }

  /* Free allocated memory */
  free(values);
  return 0;
}

int FDFStore::omap_get(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  int ret = omap_get_header(cid, oid, header, 0);
  if (ret != 0)
    return ret;

  string key;
  ghobject_to_string(oid, key);

  ZS_range_meta_t  rmeta;
  struct ZS_cursor *cursor;
  ZS_range_data_t  *values;
  int i, n_in, n_out;

  ZS_status_t rc;
  memset(&rmeta, 0, sizeof(rmeta));

  ZS_range_enums_t flags = static_cast<ZS_range_enums_t>
              (ZS_RANGE_START_GT | ZS_RANGE_END_LT);
  rmeta.flags = flags;

  string key_start = key + OMAP_SEPARATOR;
  size_t encoded_len = key_start.length();

  /*  1 is the trailing '\0' */
  rmeta.key_start = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_start);
  memcpy(rmeta.key_start, key_start.c_str(), key_start.length());
  rmeta.keylen_start = encoded_len;

  string key_end = key + COLL_ATTR_SEPARATOR;
  encoded_len = key_end.length();

  /* 1 is the trailing '\0' */
  rmeta.key_end = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_end);
  memcpy(rmeta.key_end, key_end.c_str(), key_end.length());
  rmeta.keylen_end = encoded_len;
  
  /* Initiate the range */
  rc = ZSGetRange(fdf_get_thd_state(), c->cguid, 
                      ZS_RANGE_PRIMARY_INDEX, &cursor, &rmeta);
  if( rc != ZS_SUCCESS) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
            << c->cguid << ") " << __func__ << " failed for FDFGetRange: "
            << ZSStrError(rc) << dendl;
    free(rmeta.key_start);
    free(rmeta.key_end);
    return -1;
  }

  free(rmeta.key_start);
  free(rmeta.key_end);

  n_in = 1;
  values = (ZS_range_data_t *) malloc(sizeof(ZS_range_data_t) * n_in);
  assert(values); 
  memset(values, 0, sizeof(ZS_range_data_t) * n_in);
  /* Traverse the range */

  string key_attr;
  while(1) {
    rc = ZSGetNextRange(fdf_get_thd_state(), cursor, n_in, &n_out, values);
    if (rc == ZS_SUCCESS || rc == ZS_QUERY_DONE) {
      if (n_out == 0) {
        break;
      }

      for (i = 0; i < n_out; i++) {
        key_attr.assign(values[i].key, values[i].keylen);
        if(key_attr.substr(0, key.length()) == key) {
          bufferlist bl;
          bufferptr bp = buffer::create(values[i].datalen);
          memcpy(bp.c_str(), values[i].data, values[i].datalen);
          bl.push_back(bp);

          out->insert(map<string, bufferlist>::value_type(
                                  key_attr.substr(key.length() + 2), bl));

          ZSFreeBuffer(values[i].key);
          ZSFreeBuffer(values[i].data);
        }
      }
    } else {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
              << c->cguid << ") " << __func__ << " failed for FDFGetNextRange: "
              << ZSStrError(rc) << dendl;
    }
  }
  /* Finish the enumeration */
  rc = ZSGetRangeFinish(fdf_get_thd_state(), cursor);
  if ( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid(" << c->cguid
              << ") " << __func__ << " failed for FDFGetNextRangeFinish: "
              << ZSStrError(rc) << dendl;
  }

  /* Free allocated memory */
  free(values);
  return 0;
}

int FDFStore::omap_get_header(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio ///< [in] don't assert on eio
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  ZS_status_t rc;
  char *data = NULL;
  uint64_t datalen = 0;

  string omap_header;
  ghobject_to_string(oid, omap_header);
  omap_header += OMAP_HEADER_SUFFIX;
  rc = ZSReadObject(fdf_get_thd_state(), c->cguid,
                      (char *)(omap_header.c_str()), omap_header.length(),
                      &data, &datalen);
  if (rc == ZS_OBJECT_UNKNOWN) {
    return -ENOENT;
  } else if( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
        << c->cguid << ") " << __func__ << " failed(key:" << oid
        << "), " << ZSStrError(rc) << dendl;
    return -1;
  }
  bufferptr bp = buffer::create(datalen);
  memcpy(bp.c_str(), data, datalen);
  header->push_back(bp);
  ZSFreeBuffer(data);
  return 0;
}

int FDFStore::omap_get_keys(
    coll_t cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  string key;
  ghobject_to_string(oid, key);

  ZS_status_t rc;
  ZS_range_meta_t  rmeta;
  struct ZS_cursor *cursor;
  ZS_range_data_t  *values;
  int i, n_in, n_out;

  memset(&rmeta, 0, sizeof(rmeta));

  ZS_range_enums_t flags = static_cast<ZS_range_enums_t>
              (ZS_RANGE_START_GT | ZS_RANGE_END_LT | ZS_RANGE_KEYS_ONLY);
  rmeta.flags = flags;

  string key_start = key + OMAP_SEPARATOR;
  size_t encoded_len = key_start.length();

  /*  1 is the trailing '\0' */
  rmeta.key_start = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_start);
  memcpy(rmeta.key_start, key_start.c_str(), key_start.length());
  rmeta.keylen_start = encoded_len;

  string key_end = key + COLL_ATTR_SEPARATOR;
  encoded_len = key_end.length();

  /* 1 is the trailing '\0' */
  rmeta.key_end = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_end);
  memcpy(rmeta.key_end, key_end.c_str(), key_end.length());
  rmeta.keylen_end = encoded_len;
  
  /* Initiate the range */
  rc = ZSGetRange(fdf_get_thd_state(), c->cguid, 
                      ZS_RANGE_PRIMARY_INDEX, &cursor, &rmeta);
  if( rc != ZS_SUCCESS) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
            << c->cguid << ") " << __func__ << " failed for FDFGetRange: "
            << ZSStrError(rc) << dendl;
    free(rmeta.key_start);
    free(rmeta.key_end);
    return -1;
  }

  free(rmeta.key_start);
  free(rmeta.key_end);

  n_in = 1;
  values = (ZS_range_data_t *) malloc(sizeof(ZS_range_data_t) * n_in);
  assert(values); 
  memset(values, 0, sizeof(ZS_range_data_t) * n_in);
  /* Traverse the range */

  string key_attr;
  while(1) {
    rc = ZSGetNextRange(fdf_get_thd_state(), cursor, n_in, &n_out, values);
    if( rc == ZS_SUCCESS || rc == ZS_QUERY_DONE) {
      if (n_out == 0) {
        break;
      }

      for (i = 0; i < n_out; i++) {
        key_attr.assign(values[i].key, values[i].keylen);
        if(key_attr.substr(0, key.length()) == key) {
          keys->insert(key_attr.substr(key.length() + 2));
        }
        ZSFreeBuffer(values[i].key);
      }
    } else {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
              << c->cguid << ") " << __func__ << " failed for FDFGetNextRange: "
              << ZSStrError(rc) << dendl;
      break;
    }
  }
  /* Finish the enumeration */
  rc = ZSGetRangeFinish(fdf_get_thd_state(), cursor);
  if ( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid(" << c->cguid
              << ") " << __func__ << " failed for FDFGetNextRangeFinish: "
              << ZSStrError(rc) << dendl;
  }

  /* Free allocated memory */
  free(values);
  return 0;
}

int FDFStore::omap_get_values(
    coll_t cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  char *data;
  uint64_t datalen;
  ZS_status_t rc;
  string oid_key;
  ghobject_to_string(oid, oid_key);

  // call FDFRangeQuery or FDFReadObject
  for (set<string>::iterator p = keys.begin(); p != keys.end(); ++p) {
    string attr_key = oid_key + OMAP_SEPARATOR + *p;
    uint32_t keylen = attr_key.length();
    rc = ZSReadObject(fdf_get_thd_state(), c->cguid, (char *)(attr_key.c_str()),
                        keylen, &data, &datalen);
    if( rc != ZS_SUCCESS ) {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
            << c->cguid << ") " << __func__ << " failed(key:"
            << oid << ", attr: " << *p << "), " << ZSStrError(rc) << dendl;
    } else {
      bufferlist bl;
      bufferptr bp = buffer::create(datalen);
      memcpy(bp.c_str(), data, datalen);
      bl.push_back(bp);
      out->insert(map<string, bufferlist>::value_type(*p, bl));
      ZSFreeBuffer(data);
    }
  }
  return 0;
}

int FDFStore::omap_check_keys(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  char *data;
  uint64_t datalen;
  ZS_status_t rc;
  string oid_key;
  ghobject_to_string(oid, oid_key);

  // call FDFRangeQuery or FDFReadObject
  for (set<string>::iterator p = keys.begin(); p != keys.end(); ++p) {
    string attr_key = oid_key + OMAP_SEPARATOR + *p;
    uint32_t keylen = attr_key.length();
    rc = ZSReadObject(fdf_get_thd_state(), c->cguid, (char *)(attr_key.c_str()),
                        keylen, &data, &datalen);
    if( rc != ZS_SUCCESS ) {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
            << c->cguid << ") " << __func__ << " failed(oid:"
            << oid << ", omap key: " << *p << "), " << ZSStrError(rc) << dendl;
    } else {
      out->insert(*p);
      ZSFreeBuffer(data);
    }
  }
  return 0;
}

ObjectMap::ObjectMapIterator FDFStore::get_omap_iterator(coll_t cid,
							 const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  /* Get the FDF container associated with the collection */
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return ObjectMap::ObjectMapIterator();

  map<string,bufferptr> aset;
//  int ret = getattrs(cid, oid, aset, 0);
  int ret = getattrs(cid, oid, aset);

  if (ret != 0 || aset.size() == 0)
    return ObjectMap::ObjectMapIterator();

  struct ZS_thread_state *thrd_state = fdf_get_thd_state();
  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(thrd_state, c, oid));
}


// ---------------
// write operations

int FDFStore::queue_transactions(Sequencer *osr,
				 list<Transaction*>& tls,
				 TrackedOpRef op,
				 ThreadPool::TPHandle *handle)
{
  for (list<Transaction*>::iterator p = tls.begin(); p != tls.end(); ++p) {
    // poke the TPHandle heartbeat just to exercise that code path
    if (handle) {
      handle->reset_tp_timeout();
    }
    _do_transaction(**p);
  }

  Context *on_apply = NULL, *on_apply_sync = NULL, *on_commit = NULL;
  ObjectStore::Transaction::collect_contexts(tls, &on_apply, &on_commit,
					     &on_apply_sync);
  if (on_apply_sync)
    on_apply_sync->complete(0);
  if (on_apply)
    finisher.queue(on_apply);
  if (on_commit)
    finisher.queue(on_commit);
  return 0;
}

void FDFStore::_do_transaction(Transaction& t)
{
  Transaction::iterator i = t.begin();
  int pos = 0;

  while (i.have_op()) {
    Transaction::Op *op = i.decode_op();
    int r = 0;

    switch (op->op) {
    case Transaction::OP_NOP:
      break;
    case Transaction::OP_TOUCH:
      {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	r = _touch(cid, oid);
      }
      break;
      
    case Transaction::OP_WRITE:
      {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	uint64_t off = op->off;
	uint64_t len = op->len;
	uint32_t fadvise_flags = i.get_fadvise_flags();
	bufferlist bl;
	i.decode_bl(bl);
	r = _write(cid, oid, off, len, bl, fadvise_flags);
      }
      break;
      
    case Transaction::OP_ZERO:
      {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	uint64_t off = op->off;
	uint64_t len = op->len;
	r = _zero(cid, oid, off, len);
      }
      break;
      
    case Transaction::OP_TRIMCACHE:
      {
	i.get_cid(op->cid);
	i.get_oid(op->oid);
	op->off;
	op->len;
	// deprecated, no-op
      }
      break;
      
    case Transaction::OP_TRUNCATE:
      {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	uint64_t off = op->off;
	r = _truncate(cid, oid, off);
      }
      break;
      
    case Transaction::OP_REMOVE:
      {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	r = _remove(cid, oid);
      }
      break;
      
    case Transaction::OP_SETATTR:
      {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	string name = i.decode_string();
	bufferlist bl;
	i.decode_bl(bl);
	map<string, bufferptr> to_set;
	to_set[name] = bufferptr(bl.c_str(), bl.length());
	r = _setattrs(cid, oid, to_set);
      }
      break;
      
    case Transaction::OP_SETATTRS:
      {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	map<string, bufferptr> aset;
	i.decode_attrset(aset);
	r = _setattrs(cid, oid, aset);
      }
      break;

    case Transaction::OP_RMATTR:
      {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	string name = i.decode_string();
	r = _rmattr(cid, oid, name.c_str());
      }
      break;

    case Transaction::OP_RMATTRS:
      {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	r = _rmattrs(cid, oid);
      }
      break;
      
    case Transaction::OP_CLONE:
      {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	ghobject_t noid = i.get_oid(op->oid);
	r = _clone(cid, oid, noid);
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	ghobject_t noid = i.get_oid(op->oid);
 	uint64_t off = op->off;
	uint64_t len = op->len;
	r = _clone_range(cid, oid, noid, off, len, off);
      }
      break;

    case Transaction::OP_CLONERANGE2:
      {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	ghobject_t noid = i.get_oid(op->oid);
 	uint64_t srcoff = op->off;
	uint64_t len = op->len;
 	uint64_t dstoff = op->off;
	r = _clone_range(cid, oid, noid, srcoff, len, dstoff);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	coll_t cid = i.get_cid(op->cid);
	r = _create_collection(cid);
      }
      break;

    case Transaction::OP_RMCOLL:
      {
	coll_t cid = i.get_cid(op->cid);
	r = _destroy_collection(cid);
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
	coll_t ncid = i.get_cid(op->cid);
	coll_t ocid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	r = _collection_add(ncid, ocid, oid);
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	r = _remove(cid, oid);
       }
      break;

    case Transaction::OP_COLL_MOVE:
      assert(0 == "deprecated");
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
	coll_t oldcid = i.get_cid(op->cid);
	ghobject_t oldoid = i.get_oid(op->oid);
	coll_t newcid = i.get_cid(op->cid);
	ghobject_t newoid = i.get_oid(op->oid);
	r = _collection_move_rename(oldcid, oldoid, newcid, newoid);
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      {
	coll_t cid = i.get_cid(op->cid);
	string name = i.decode_string();
	bufferlist bl;
	i.decode_bl(bl);
	r = _collection_setattr(cid, name.c_str(), bl.c_str(), bl.length());
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      {
	coll_t cid = i.get_cid(op->cid);
	string name = i.decode_string();
	r = _collection_rmattr(cid, name.c_str());
      }
      break;

    case Transaction::OP_COLL_RENAME:
      {
	coll_t cid(i.get_cid(op->cid));
	coll_t ncid(i.get_cid(op->cid));
	r = _collection_rename(cid, ncid);
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
	coll_t cid(i.get_cid(op->cid));
	ghobject_t oid = i.get_oid(op->oid);
	r = _omap_clear(cid, oid);
      }
      break;
    case Transaction::OP_OMAP_SETKEYS:
      {
	coll_t cid(i.get_cid(op->cid));
	ghobject_t oid = i.get_oid(op->oid);
	map<string, bufferlist> aset;
	i.decode_attrset(aset);
	r = _omap_setkeys(cid, oid, aset);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
	coll_t cid(i.get_cid(op->cid));
	ghobject_t oid = i.get_oid(op->oid);
	set<string> keys;
	i.decode_keyset(keys);
	r = _omap_rmkeys(cid, oid, keys);
      }
      break;
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
	coll_t cid(i.get_cid(op->cid));
	ghobject_t oid = i.get_oid(op->oid);
	string first, last;
	first = i.decode_string();
	last = i.decode_string();
	r = _omap_rmkeyrange(cid, oid, first, last);
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
	coll_t cid(i.get_cid(op->cid));
	ghobject_t oid = i.get_oid(op->oid);
	bufferlist bl;
	i.decode_bl(bl);
	r = _omap_setheader(cid, oid, bl);
      }
      break;
    case Transaction::OP_SPLIT_COLLECTION:
      assert(0 == "deprecated");
      break;
    case Transaction::OP_SPLIT_COLLECTION2:
      {
	coll_t cid(i.get_cid(op->cid));
        uint32_t bits = op->split_bits;
        uint32_t rem = op->split_rem;
	coll_t dest(i.get_cid(op->cid));
	r = _split_collection(cid, bits, rem, dest);
      }
      break;
    case Transaction::OP_COLL_HINT:
       {
        coll_t cid = i.get_cid(op->cid);
        uint32_t type = op->hint_type;
        bufferlist hint;
        i.decode_bl(hint);
        bufferlist::iterator hiter = hint.begin();
        if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
          uint32_t pg_num;
          uint64_t num_objs;
          ::decode(pg_num, hiter);
          ::decode(num_objs, hiter);
          r = _collection_hint_expected_num_objs(cid, pg_num, num_objs);
        } else {
          // Ignore the hint
          dout(10) << "Unrecognized collection hint type: " << type << dendl;
        }
      }
      break;
    case Transaction::OP_SETALLOCHINT:
      break;

    default:
      derr << "bad op " << op << dendl;
      assert(0);
    }

    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(op->op == Transaction::OP_CLONERANGE ||
			    op->op == Transaction::OP_CLONE ||
			    op->op == Transaction::OP_CLONERANGE2 ||
			    op->op == Transaction::OP_COLL_ADD))
	// -ENOENT is usually okay
	ok = true;
      if (r == -ENODATA)
	ok = true;

      if (!ok) {
	const char *msg = "unexpected error code";

	if (r == -ENOENT && (op->op == Transaction::OP_CLONERANGE ||
			     op->op == Transaction::OP_CLONE ||
			     op->op == Transaction::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	if (r == -ENOSPC)
	  // For now, if we hit _any_ ENOSPC, crash, before we do any damage
	  // by partially applying transactions.
	  msg = "ENOSPC handling not implemented";

	if (r == -ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	}

	dout(0) << " error " << cpp_strerror(r) << " not handled on operation " << op
		<< " (op " << pos << ", counting from 0)" << dendl;
	dout(0) << msg << dendl;
	dout(0) << " transaction dump:\n";
	JSONFormatter f(true);
	f.open_object_section("transaction");
	t.dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;
	assert(0 == "unexpected error");
      }
    }

    ++pos;
  }
//tomy wc
if(p_num_obj) _mput();
}

int FDFStore::_mput()
{
  ZS_status_t rc = ZS_FAILURE;
  uint32_t obj_mput = 0;
//dout(0) << "tomy: _mput #objects: " << p_num_obj << dendl;
  assert(p_num_obj > 0);
//dout(0) << "fdf_wc_mput: " << p_num_obj << dendl;
  rc = ZSMPut(fdf_get_thd_state(), p_cguid, p_num_obj, p_obj, 0, &obj_mput);
  if( rc != ZS_SUCCESS ) {
        dout(0) << "_mput failed" << dendl;
  }

  if(p_num_obj != obj_mput){
      //dout(0) << "tomy: p_num_obj: " << p_num_obj << "obj_mput: " << obj_mput << dendl;
  }

  //assert(p_num_obj == obj_mput);
  uint32_t i = 0;
  for(;i<p_num_obj;i++){
        free(p_obj[i].key);
        free(p_obj[i].data);
  }
  p_num_obj = 0;
  p_cguid = 0;
  p_prefix[0] = '\0';
//  memset(prepend, 0, 256);
  return 0;
}

int FDFStore::_touch(coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid
           << " not needed for FDF" << dendl;

  return 0;
}

int FDFStore::_write(coll_t cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, const bufferlist& bl,
		     bool replica)
{
    dout(10) << __func__ << " " << cid << " " << oid << " "
	<< offset << "~" << len << dendl;
    assert(len == bl.length());

    ZS_status_t rc;
    string key;
    ghobject_to_string(oid, key);
    key = key + DATA_SEPARATOR;

    /* Get the FDF container associated with the collection */
    CollectionRef c = fdf_get_container_for_coll(cid);
    if (!c)
	return -ENOENT;
#ifndef SHIM_TESSELLATION_ENABLE
    char *data = NULL;
    uint64_t datalen = 0;
    /* read the object */
    rc = ZSReadObject(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
	    key.length(), &data, &datalen);
    if(rc == ZS_SUCCESS){
	if(datalen > offset+len){
	    assert(data != NULL);
	    memcpy(data+offset, const_cast <bufferlist&>(bl).c_str(), len);
//tomy wc
fdf_write_to_mput_wrapper(c->cguid, (char *)key.c_str(), key.length(), data, datalen);
if(data) ZSFreeBuffer(data);
/*
	    rc = FDFWriteObject(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		    key.length(), data, datalen, 0);
	    if(data) FDFFreeBuffer(data);
	    if( rc != FDF_SUCCESS ) {
		return -1;
	    }
*/
	} else {
	    char *wdata = (char *) malloc(offset+len * sizeof(char));
	    memset(wdata, 0, offset+len);
	    if(data){
		memcpy(wdata, data, datalen);
	    }
	    memcpy(wdata+offset, const_cast <bufferlist&>(bl).c_str(), len);
	    if(data) ZSFreeBuffer(data);
//tomy wc
fdf_write_to_mput_wrapper(c->cguid, (char *)key.c_str(), key.length(), wdata, offset+len);
if(wdata) free(wdata);
/*
	    rc = FDFWriteObject(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		    key.length(), wdata, offset+len, 0);
	    if(wdata) free(wdata);
	    if( rc != FDF_SUCCESS ) {
		return -1;
	    }
*/
	}
    } else if (rc == ZS_OBJECT_UNKNOWN){
//tomy wc
fdf_write_to_mput_wrapper(c->cguid, (char *)key.c_str(), key.length(),
			    const_cast <bufferlist&>(bl).c_str(), len);
/*
	rc = FDFWriteObject(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), const_cast <bufferlist&>(bl).c_str(), len, 0);
	if( rc != FDF_SUCCESS ) {
	    return -1;
	}
*/
    } else return -1;
#else //SHIM_TESSELLATION_ENABLE
    int exists = 0;
    int tess = 0;
    uint64_t datalen = 0;
    dout(0) << __func__ << " Key: " << key << dendl;
    rc = fdf_object_exists(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
	    key.length(), &exists, &tess, &datalen);
    //assert((FDF_SUCCESS|FDF_QUERY_DONE) == rc);

    if(!exists){
	rc = fdf_object_split(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), const_cast <bufferlist&>(bl).c_str(),
		((bl.length() < len) ? bl.length() : len), offset);
	assert(ZS_SUCCESS == rc);
    } else {
	char **fdfov = NULL;
	int ovcnt = 0;
	rc = fdf_object_list(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), offset, len, &fdfov, &ovcnt);
	
	if(ovcnt == 0){
		rc = fdf_object_split(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
			key.length(), const_cast <bufferlist&>(bl).c_str(),
			((bl.length() < len) ? bl.length() : len), offset);
		assert(ZS_SUCCESS == rc);
	} else {
	//assert((FDF_SUCCESS|FDF_QUERY_DONE) == rc);
	uint64_t so = offset;
	uint64_t eo = offset+len-1;

	rc = fdf_object_range(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), offset, len, fdfov, ovcnt, &so, &eo);
	assert(ZS_SUCCESS == rc);

	char *tmpdata = NULL;

	rc = fdf_object_join(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), so, eo-so+1, &tmpdata, fdfov, ovcnt);
	assert(ZS_SUCCESS == rc);

	rc = fdf_object_delete_list(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), fdfov, ovcnt);
	assert(ZS_SUCCESS == rc);

	fdf_object_free_list(fdfov, ovcnt);
	free(fdfov);

	memcpy((tmpdata+(offset - so)), const_cast <bufferlist&>(bl).c_str(),
		((bl.length() < len) ? bl.length() : len));
	rc = fdf_object_split(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), tmpdata, eo-so+1, so);
	assert(ZS_SUCCESS == rc);

	free(tmpdata);
	}
    }
#endif //SHIM_TESSELLATION_ENABLE
    return 0;
}

void FDFStore::_write_into_bl(const bufferlist& src, unsigned offset,
			      bufferlist *dst)
{
  unsigned len = src.length();

  // before
  bufferlist newdata;
  if (dst->length() >= offset) {
    newdata.substr_of(*dst, 0, offset);
  } else {
    newdata.substr_of(*dst, 0, dst->length());
    bufferptr bp(offset - dst->length());
    bp.zero();
    newdata.append(bp);
  }

  newdata.append(src);

  // after
  if (dst->length() > offset + len) {
    bufferlist tail;
    tail.substr_of(*dst, offset + len, dst->length() - (offset + len));
    newdata.append(tail);
  }

  dst->claim(newdata);
}

int FDFStore::_zero(coll_t cid, const ghobject_t& oid,
		    uint64_t offset, size_t len)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << offset << "~"
	   << len << dendl;
  bufferptr bp(len);
  bp.zero();
  bufferlist bl;
  bl.push_back(bp);
  return _write(cid, oid, offset, len, bl);
}

int FDFStore::_truncate(coll_t cid, const ghobject_t& oid, uint64_t size)
{
    dout(10) << __func__ << " " << cid << " " << oid << " " << size << dendl;
    /* Get the FDF container associated with the collection */
    CollectionRef c = fdf_get_container_for_coll(cid);
    if (!c)
	return -ENOENT;

    string key;
    ghobject_to_string(oid, key);
    key = key + DATA_SEPARATOR;

    ZS_status_t rc;
#ifndef SHIM_TESSELLATION_ENABLE
    char *data = NULL;
    uint64_t datalen = 0;
    rc = ZSReadObject(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
	    key.length(), &data, &datalen);

    if (rc == ZS_OBJECT_UNKNOWN) {
	return 0;
    } else if( rc != ZS_SUCCESS ) {
	dout(10) << "FDF container(" << cid.to_str() << "), cguid("
	    << c->cguid << ") " << __func__ << " failed(key:"
	    << oid << "), " << ZSStrError(rc) << dendl;
	return -1;
    }

    if (datalen > size) {
//tomy wc 
fdf_write_to_mput_wrapper(c->cguid, (char *)key.c_str(), key.length(), data, size);
/*
	rc = FDFWriteObject(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), data, size, 0);
*/
    } else if (datalen == size) {
	ZSFreeBuffer(data);
	return 0;
    } else {
	char *new_data = new char[size];
	if (new_data == NULL) {
	    derr << "malloc failed in " << __func__ << " function" << dendl;
	    ZSFreeBuffer(data);
	    return -1;
	}
	memset(new_data, 0, size);
	memcpy(new_data, data, datalen);
//tomy wc
fdf_write_to_mput_wrapper(c->cguid, (char *)key.c_str(), key.length(), new_data, size);
delete[] new_data;
/*
	rc = FDFWriteObject(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), new_data, size, 0);
	delete[] new_data;
    }
    if( rc != FDF_SUCCESS ) {
	dout(10) << "FDF container(" << cid.to_str() << "), cguid("
	    << c->cguid << ") " << __func__ << " failed(key:"
	    << oid << "), " << FDFStrError(rc) << dendl;
	FDFFreeBuffer(data);
	return -1;
    }
*/
    ZSFreeBuffer(data);
    return 0;
#else //SHIM_TESSELLATION_ENABLE
    int exists = 0;
    int tess = 0;
    uint64_t datalen = 0;

    rc = fdf_object_exists(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
	    key.length(), &exists, &tess, &datalen);
    //assert((FDF_SUCCESS|FDF_QUERY_DONE) == rc);
    if(!exists) return 0;

    assert(tess == 1);

    if(size == datalen){
	return 0;
    }

    if(size < datalen){
	char **fdfov = NULL;
	int ovcnt = 0;

	rc = fdf_object_list(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), size ? size-1 : size, datalen - size , &fdfov, &ovcnt);
	//assert((FDF_SUCCESS|FDF_QUERY_DONE) == rc);
	//assert(ovcnt > 0);
    if (ovcnt > 0) return 0;
	if(ovcnt > 1){
	    rc = fdf_object_delete_list(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		    key.length(), &fdfov[1], ovcnt-1);
	    assert(ZS_SUCCESS == rc);
	}
	uint64_t so = 0;
	uint64_t eo = 0;
	char tob[maxosz];
	memset(tob, 0, maxosz);
	memcpy(tob, fdfov[0]+key.length(), maxosz);
	so = atoll(tob);
	memset(tob, 0, maxosz);
	memcpy(tob, fdfov[0]+key.length()+maxosz, maxosz);
	eo = atoll(tob);
	char *tmpdata = NULL;
	rc = fdf_object_join(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), so, (eo-so+1), &tmpdata, &fdfov[0], 1);
	assert(ZS_SUCCESS == rc);

	rc = fdf_object_delete_list(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), &fdfov[0], 1);
	assert(ZS_SUCCESS == rc);

	rc = fdf_object_split(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), tmpdata, size, so);
	assert(ZS_SUCCESS == rc);

	free(tmpdata);
	fdf_object_free_list(fdfov, ovcnt);
	free(fdfov);
    } else {
	char *tmpdata = (char *) calloc((size - datalen), sizeof(char));
	rc = fdf_object_split(fdf_get_thd_state(), c->cguid, (char *)key.c_str(),
		key.length(), tmpdata, size - datalen, datalen);
	free(tmpdata);
	assert(ZS_SUCCESS == rc);
    }
    return 0;
#endif //SHIM_TESSELLATION_ENABLE
}

int FDFStore::_remove(coll_t cid, const ghobject_t& oid)
{
    dout(10) << __func__ << " " << cid << " " << oid << dendl;

    /* Get the FDF container associated with the collection */
    CollectionRef c = fdf_get_container_for_coll(cid);
    if (!c)
	return -ENOENT;

    ZS_status_t rc;
    string key, key1;
    ghobject_to_string(oid, key);
    key1 = key + DATA_SEPARATOR;
#ifndef SHIM_TESSELLATION_ENABLE
    rc = ZSDeleteObject(fdf_get_thd_state(), c->cguid,
	    (char *)(key.c_str()), key.length());
    if (rc == ZS_OBJECT_UNKNOWN) {
	//return -ENOENT;
    } else if( rc != ZS_SUCCESS ) {
	dout(10) << "FDF container(" << cid.to_str() << "), cguid("
	    << c->cguid << ") " << __func__ << " failed(key:" << oid
	    << ZSStrError(rc) << dendl;
	//return -1;
    }
#else //SHIM_TESSELLATION_ENABLE
    int exists = 0;
    int tess = 0;
    uint64_t datalen = 0;
    rc = fdf_object_exists(fdf_get_thd_state(), c->cguid, (char *)(key1.c_str()),
	    key1.length(), &exists, &tess, &datalen);
    //assert((FDF_SUCCESS|FDF_QUERY_DONE) == rc);

    if(!exists){
	//return 0;
    } else {
	assert(tess == 1);
	char **fdfov = NULL;
	int ovcnt = 0;
	rc = fdf_object_list(fdf_get_thd_state(), c->cguid, (char *)(key1.c_str()),
		key1.length(), 0, datalen, &fdfov, &ovcnt);
	//assert((FDF_SUCCESS|FDF_QUERY_DONE) == rc);

	//assert(ovcnt > 0);
    if (ovcnt > 0) return 0;
	rc = fdf_object_delete_list(fdf_get_thd_state(), c->cguid, (char *)(key1.c_str()),
		key1.length(), fdfov, ovcnt);
	assert(ZS_SUCCESS == rc);

	fdf_object_free_list(fdfov, ovcnt);
	free(fdfov);
	//return 0;
    }

#endif //SHIM_TESSELLATION_ENABLE

  // also delete the key attributes and omap
  ZS_range_meta_t  rmeta;
  struct ZS_cursor *cursor;
  ZS_range_data_t  *values;
  int i, n_in, n_out;

  memset(&rmeta, 0, sizeof(rmeta));

  ZS_range_enums_t flags = static_cast<ZS_range_enums_t>
              (ZS_RANGE_START_GT | ZS_RANGE_END_LT | ZS_RANGE_KEYS_ONLY);
  rmeta.flags = flags;

  string key_start = key + ATTR_SEPARATOR;
  size_t encoded_len = key_start.length();

  /*  1 is the trailing '\0' */
  rmeta.key_start = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_start);
  memcpy(rmeta.key_start, key_start.c_str(), key_start.length());
  rmeta.keylen_start = encoded_len;

  string key_end = key + COLL_ATTR_SEPARATOR;
  encoded_len = key_end.length();

  /* 1 is the trailing '\0' */
  rmeta.key_end = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_end);
  memcpy(rmeta.key_end, key_end.c_str(), key_end.length());
  rmeta.keylen_end = encoded_len;
  
  /* Initiate the range */
  rc = ZSGetRange(fdf_get_thd_state(), c->cguid, 
                      ZS_RANGE_PRIMARY_INDEX, &cursor, &rmeta);
  if( rc != ZS_SUCCESS) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
            << c->cguid << ") " << __func__ << " failed for FDFGetRange: "
            << ZSStrError(rc) << dendl;
    free(rmeta.key_start);
    free(rmeta.key_end);
    return -1;
  }

  free(rmeta.key_start);
  free(rmeta.key_end);

  n_in = 1;
  values = (ZS_range_data_t *) malloc(sizeof(ZS_range_data_t) * n_in);
  assert(values); 
  memset(values, 0, sizeof(ZS_range_data_t) * n_in);
  /* Traverse the range */
  while(1) {
    rc = ZSGetNextRange(fdf_get_thd_state(), cursor, n_in, &n_out, values);
    if( rc == ZS_SUCCESS || rc == ZS_QUERY_DONE) {
      if (n_out == 0) {
        break;
      }

      for (i = 0; i < n_out; i++) {
        rc = ZSDeleteObject(fdf_get_thd_state(), c->cguid,
                            values[i].key, values[i].keylen);
        if( rc != ZS_SUCCESS ) {
          dout(10) << "FDF container(" << cid.to_str() << "), cguid("
              << c->cguid << ") " << __func__ << " failed(key:" << oid
              << ZSStrError(rc) << dendl;
         }
         ZSFreeBuffer(values[i].key);
      }
    } else {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
              << c->cguid << ") " << __func__ << " failed for FDFGetNextRange: "
              << ZSStrError(rc) << dendl;
      break;
    }
  }
  /* Finish the enumeration */
  rc = ZSGetRangeFinish(fdf_get_thd_state(), cursor);
  if ( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid(" << c->cguid
              << ") " << __func__ << " failed for FDFGetNextRangeFinish: "
              << ZSStrError(rc) << dendl;
  }

  /* Free allocated memory */
  free(values);
  return 0;
}

int FDFStore::_setattrs(coll_t cid, const ghobject_t& oid,
			map<string,bufferptr>& aset)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  ZS_status_t rc = ZS_SUCCESS;

  /* Get the FDF container associated with the collection */
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  string key, key_attr;
  ghobject_to_string(oid, key);
  /* Set the attributes */
  for (map<string,bufferptr>::iterator p = aset.begin(); p != aset.end(); ++p) {
    /* Encode the object attributes */
    key_attr = key + ATTR_SEPARATOR + p->first;
//tomy wc
fdf_write_to_mput_wrapper(c->cguid, (char *)key_attr.c_str(), key_attr.length(),
    (char *)p->second.c_str(), p->second.length());
/*
    rc = FDFWriteObject(fdf_get_thd_state(), c->cguid,
                        (char *)key_attr.c_str(), key_attr.length(),
                        (char *)p->second.c_str(), p->second.length(), 0);
    if( rc != FDF_SUCCESS ) {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
          << c->cguid << ") " << __func__ << " failed(attr:" << p->first
          << " val:" << p->second << "), " << FDFStrError(rc) << dendl;
      aset.clear();
      return -1;
    }
*/
  }

  return 0;
}

int FDFStore::_rmattr(coll_t cid, const ghobject_t& oid, const char *name)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << name << dendl;
  ZS_status_t rc = ZS_SUCCESS;

  /* Get the FDF container associated with the collection */
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

    /* Encode the object attributes */
  string key_attr;
  ghobject_to_string(oid, key_attr);
  key_attr = key_attr + ATTR_SEPARATOR + name;

  rc = ZSDeleteObject(fdf_get_thd_state(), c->cguid,
                       (char *)key_attr.c_str(), key_attr.length());
  if( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid << "), cguid("
         << c->cguid << ") " << __func__ << " failed(key:" << name
         << "), " << ZSStrError(rc) << dendl;
    return -1;
  }

  return 0;
}

int FDFStore::_rmattrs(coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  string key;
  ghobject_to_string(oid, key);

  ZS_range_meta_t  rmeta;
  struct ZS_cursor *cursor;
  ZS_range_data_t  *values;
  int i, n_in, n_out;

  ZS_status_t rc;
  memset(&rmeta, 0, sizeof(rmeta));

  ZS_range_enums_t flags = static_cast<ZS_range_enums_t>
              (ZS_RANGE_START_GT | ZS_RANGE_END_LT | ZS_RANGE_KEYS_ONLY);
  rmeta.flags = flags;

  string key_start = key + ATTR_SEPARATOR;
  size_t encoded_len = key_start.length();

  /*  1 is the trailing '\0' */
  rmeta.key_start = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_start);
  memcpy(rmeta.key_start, key_start.c_str(), key_start.length());
  rmeta.keylen_start = encoded_len;

  string key_end = key + OMAP_SEPARATOR;
  encoded_len = key_end.length();
  /* 1 is the trailing '\0' */
  rmeta.key_end = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_end);
  memcpy(rmeta.key_end, key_end.c_str(), key_end.length());
  rmeta.keylen_end = encoded_len;
  
  /* Initiate the range */
  rc = ZSGetRange(fdf_get_thd_state(), c->cguid, 
                      ZS_RANGE_PRIMARY_INDEX, &cursor, &rmeta);
  if( rc != ZS_SUCCESS) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
            << c->cguid << ") " << __func__ << " failed for FDFGetRange: "
            << ZSStrError(rc) << dendl;
    free(rmeta.key_start);
    free(rmeta.key_end);
    return -1;
  }

  free(rmeta.key_start);
  free(rmeta.key_end);

  n_in = 1;
  values = (ZS_range_data_t *) malloc(sizeof(ZS_range_data_t) * n_in);
  assert(values); 
  memset(values, 0, sizeof(ZS_range_data_t) * n_in);
  /* Traverse the range */
  while(1) {
    rc = ZSGetNextRange(fdf_get_thd_state(), cursor, n_in, &n_out, values);
    if (rc == ZS_SUCCESS || rc == ZS_QUERY_DONE) {
      if (n_out == 0) {
        break;
      }

      for (i = 0; i < n_out; i++) {
        rc = ZSDeleteObject(fdf_get_thd_state(), c->cguid,
                            values[i].key, values[i].keylen);
        if( rc != ZS_SUCCESS ) {
          dout(10) << "FDF container(" << cid.to_str() << "), cguid("
              << c->cguid << ") " << __func__ << " failed(key:" << oid
              << ZSStrError(rc) << dendl;
         }
         ZSFreeBuffer(values[i].key);
      }
    } else {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
              << c->cguid << ") " << __func__ << " failed for FDFGetNextRange: "
              << ZSStrError(rc) << dendl;
    }
  }
  /* Finish the enumeration */
  rc = ZSGetRangeFinish(fdf_get_thd_state(), cursor);
  if ( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid(" << c->cguid
              << ") " << __func__ << " failed for FDFGetNextRangeFinish: "
              << ZSStrError(rc) << dendl;
  }

  /* Free allocated memory */
  free(values);
  return 0;
}

int FDFStore::_clone(coll_t cid, const ghobject_t& oldoid,
		     const ghobject_t& newoid)
{
  dout(10) << __func__ << " " << cid << " " << oldoid
	   << " -> " << newoid << "not implemented yet" << dendl;
  return 0;
}

int FDFStore::_clone_range(coll_t cid, const ghobject_t& oldoid,
			   const ghobject_t& newoid,
			   uint64_t srcoff, uint64_t len, uint64_t dstoff)
{
  dout(10) << __func__ << " " << cid << " "
	   << oldoid << " " << srcoff << "~" << len << " -> "
	   << newoid << " " << dstoff << "~" << len
	   << " not implemented yet" << dendl;
  return len;
}

int FDFStore::_omap_clear(coll_t cid, const ghobject_t &oid)
{
//#if 0
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  ZS_status_t rc;
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  ZS_range_meta_t  rmeta;
  struct ZS_cursor *cursor;
  ZS_range_data_t  *values;
  int i, n_in, n_out;

  memset(&rmeta, 0, sizeof(rmeta));

  ZS_range_enums_t flags = static_cast<ZS_range_enums_t>
              (ZS_RANGE_START_GT | ZS_RANGE_END_LT | ZS_RANGE_KEYS_ONLY);
  rmeta.flags = flags;

  string key;
  ghobject_to_string(oid, key);

  string key_start = key + OMAP_SEPARATOR;
  size_t encoded_len = key_start.length();

  /*  1 is the trailing '\0' */
  rmeta.key_start = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_start);
  memcpy(rmeta.key_start, key_start.c_str(), key_start.length());
  rmeta.keylen_start = encoded_len;

  string key_end = key + COLL_ATTR_SEPARATOR;
  encoded_len = key_end.length();

  /* 1 is the trailing '\0' */
  rmeta.key_end = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_end);
  memcpy(rmeta.key_end, key_end.c_str(), key_end.length());
  rmeta.keylen_end = encoded_len;
  
  /* Initiate the range */
  rc = ZSGetRange(fdf_get_thd_state(), c->cguid, 
                      ZS_RANGE_PRIMARY_INDEX, &cursor, &rmeta);
  if( rc != ZS_SUCCESS) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
            << c->cguid << ") " << __func__ << " failed for FDFGetRange: "
            << ZSStrError(rc) << dendl;
    free(rmeta.key_start);
    free(rmeta.key_end);
    return -1;
  }

  free(rmeta.key_start);
  free(rmeta.key_end);

  n_in = 1;
  values = (ZS_range_data_t *) malloc(sizeof(ZS_range_data_t) * n_in);
  assert(values); 
  memset(values, 0, sizeof(ZS_range_data_t) * n_in);
  /* Traverse the range */
  while(1) {
    rc = ZSGetNextRange(fdf_get_thd_state(), cursor, n_in, &n_out, values);
    if( rc == ZS_SUCCESS || rc == ZS_QUERY_DONE) {
      if (n_out == 0) {
        break;
      }

      for (i = 0; i < n_out; i++) {
        rc = ZSDeleteObject(fdf_get_thd_state(), c->cguid,
                            values[i].key, values[i].keylen);
        if( rc != ZS_SUCCESS ) {
          dout(10) << "FDF container(" << cid.to_str() << "), cguid("
              << c->cguid << ") " << __func__ << " failed(key:" << oid
              << ZSStrError(rc) << dendl;
         }
         ZSFreeBuffer(values[i].key);
      }
    } else {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
              << c->cguid << ") " << __func__ << " failed for FDFGetNextRange: "
              << ZSStrError(rc) << dendl;
      break;
    }
  }
  /* Finish the enumeration */
  rc = ZSGetRangeFinish(fdf_get_thd_state(), cursor);
  if ( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid(" << c->cguid
              << ") " << __func__ << " failed for FDFGetNextRangeFinish: "
              << ZSStrError(rc) << dendl;
  }

  /* Free allocated memory */
  free(values);
//#endif
  return 0;
}

int FDFStore::_omap_setkeys(coll_t cid, const ghobject_t &oid,
			    const map<string, bufferlist> &aset)
{
//#if 0
  dout(10) << __func__ << " " << cid << " " << oid << dendl;

  ZS_status_t rc;
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  string oid_key;
  ghobject_to_string(oid, oid_key);
  for (map<string,bufferlist>::const_iterator p = aset.begin();
    p != aset.end();
    ++p) {
    string key = oid_key + OMAP_SEPARATOR + p->first;

    uint32_t keylen = key.length();

    char *val = (char *)const_cast<bufferlist&>(p->second).c_str();
    uint64_t vallen =  const_cast<bufferlist&>(p->second).length();
    char tbuf[256];
    memset(tbuf, 0, 256);
#if 0
    if(strncmp((char *)(key.c_str()), "pglog", 5) == NULL){ 
        if( strncmp(prepend, "infos/head", 10) == NULL){
            strncpy(tbuf, prepend, strlen(prepend)+1);
            strcat(tbuf, (char *)(key.c_str()));
            uint32_t keylen1 = strlen(tbuf);
            //dout(0) <<"tomy: prepeded "<< prepend << dendl;
            fdf_write_to_mput_wrapper(c->cguid, tbuf, keylen1, val, vallen);
        } else {
            fdf_write_to_mput_wrapper(c->cguid, (char *)(key.c_str()), keylen, val, vallen);
        }
    } else {
        fdf_write_to_mput_wrapper(c->cguid, (char *)(key.c_str()), keylen, val, vallen);
    }
#endif
fdf_write_to_mput_wrapper(c->cguid, (char *)(key.c_str()), keylen, val, vallen);
//tomy wc
//fdf_write_to_mput_wrapper(c->cguid, (char *)(key.c_str()), keylen, val, vallen);
/*
    rc = FDFWriteObject(fdf_get_thd_state(), c->cguid,
                        (char *)(key.c_str()), keylen, val, vallen, 0);
    if( rc != FDF_SUCCESS ) {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
          << c->cguid << ") " << __func__ << " failed(key:" << p->first
          << " val:" << p->second << "), " << FDFStrError(rc) << dendl;
      assert(0);
    }
*/
#if 0
  if (strncmp((char *)(key.c_str()), "infos/head", 10) == NULL){
      memset(prepend, 0, 256);
      strncpy(prepend, (char *)(key.c_str()), keylen);
      char *tmpchar = strstr(prepend, "_");
      if(tmpchar) *(tmpchar) = '\0';
  }
#endif
  }
//#endif
  return 0;
}

int FDFStore::_omap_rmkeys(coll_t cid, const ghobject_t &oid,
			   const set<string> &keys)
{
//#if 0
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  ZS_status_t rc;
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  string oid_key;
  ghobject_to_string(oid, oid_key);
  for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p) {
    string key = oid_key + OMAP_SEPARATOR + *p;
    uint32_t keylen = key.length();

    rc = ZSDeleteObject(fdf_get_thd_state(), c->cguid,
                        (char *)(key.c_str()), keylen);
    if( rc != ZS_SUCCESS ) {
      dout(0) << "tomy: FDF container(" << cid.to_str() << "), cguid("
          << c->cguid << ") " << __func__ << " failed(key:" << oid_key
          << ", omap key: " << *p << "), " << ZSStrError(rc) << dendl;
      //return -1;
    }
  }
//#endif
  return 0;
}

int FDFStore::_omap_rmkeyrange(coll_t cid, const ghobject_t &oid,
			       const string& first, const string& last)
{
//#if 0
  dout(10) << __func__ << " " << cid << " " << oid << " " << first
	   << " " << last << dendl;
  ZS_status_t rc;
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  string key;
  ghobject_to_string(oid, key);

  ZS_range_meta_t  rmeta;
  struct ZS_cursor *cursor;
  ZS_range_data_t  *values;
  int i, n_in, n_out;

  memset(&rmeta, 0, sizeof(rmeta));

  ZS_range_enums_t flags = static_cast<ZS_range_enums_t>
              (ZS_RANGE_START_GT | ZS_RANGE_END_LT | ZS_RANGE_KEYS_ONLY);
  rmeta.flags = flags;

  string key_start = key + OMAP_SEPARATOR + first;
  size_t encoded_len = key_start.length();

  /*  1 is the trailing '\0' */
  rmeta.key_start = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_start);
  memcpy(rmeta.key_start, key_start.c_str(), key_start.length());
  rmeta.keylen_start = encoded_len;

  string key_end = key + OMAP_SEPARATOR + first;
  encoded_len = key_end.length();

  /* 1 is the trailing '\0' */
  rmeta.key_end = (char *)calloc(1, (encoded_len + 1));
  assert(rmeta.key_end);
  memcpy(rmeta.key_end, key_end.c_str(), key_end.length());
  rmeta.keylen_end = encoded_len;
  
  /* Initiate the range */
  rc = ZSGetRange(fdf_get_thd_state(), c->cguid, 
                      ZS_RANGE_PRIMARY_INDEX, &cursor, &rmeta);
  if( rc != ZS_SUCCESS) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
            << c->cguid << ") " << __func__ << " failed for FDFGetRange: "
            << ZSStrError(rc) << dendl;
    free(rmeta.key_start);
    free(rmeta.key_end);
    return -1;
  }

  free(rmeta.key_start);
  free(rmeta.key_end);

  n_in = 1;
  values = (ZS_range_data_t *) malloc(sizeof(ZS_range_data_t) * n_in);
  assert(values); 
  memset(values, 0, sizeof(ZS_range_data_t) * n_in);
  /* Traverse the range */
  while(1) {
    rc = ZSGetNextRange(fdf_get_thd_state(), cursor, n_in, &n_out, values);
    if( rc == ZS_SUCCESS || rc == ZS_QUERY_DONE) {
      if (n_out == 0) {
        break;
      }

      for (i = 0; i < n_out; i++) {
        rc = ZSDeleteObject(fdf_get_thd_state(), c->cguid,
                            values[i].key, values[i].keylen);
        if( rc != ZS_SUCCESS ) {
          dout(10) << "FDF container(" << cid.to_str() << "), cguid("
              << c->cguid << ") " << __func__ << " failed(key:" << oid
              << ZSStrError(rc) << dendl;
         }
         ZSFreeBuffer(values[i].key);
      }
    } else {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
              << c->cguid << ") " << __func__ << " failed for FDFGetNextRange: "
              << ZSStrError(rc) << dendl;
      break;
    }
  }
  /* Finish the enumeration */
  rc = ZSGetRangeFinish(fdf_get_thd_state(), cursor);
  if ( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid(" << c->cguid
              << ") " << __func__ << " failed for FDFGetNextRangeFinish: "
              << ZSStrError(rc) << dendl;
  }

  /* Free allocated memory */
  free(values);
//#endif
  return 0;
}

int FDFStore::_omap_setheader(coll_t cid, const ghobject_t &oid,
			      const bufferlist &bl)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = fdf_get_container_for_coll(cid);
  if (!c)
    return -ENOENT;

  ZS_status_t rc;
  string omap_header;
  ghobject_to_string(oid, omap_header);
  omap_header += OMAP_HEADER_SUFFIX;

//tomy wc
fdf_write_to_mput_wrapper(c->cguid, (char *)omap_header.c_str(), omap_header.length(),
    const_cast <bufferlist&>(bl).c_str(), bl.length());
/*
  rc = FDFWriteObject(fdf_get_thd_state(), c->cguid,
                      (char *)omap_header.c_str(), omap_header.length(),
                      const_cast <bufferlist&>(bl).c_str(), bl.length(), 0);
  if( rc != FDF_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
        << c->cguid << ") " << __func__ << " failed(key:" << oid
        << "), " << FDFStrError(rc) << dendl;
    return -1;
  }
*/
  return 0;
}

int FDFStore::_create_collection(coll_t cid)
{
  CollectionRef c = get_collection(cid);
  if (c) {
    return -EEXIST;
  }

  ZS_status_t rc;

  assert(fdf_get_thd_state());

  /* Create/Open container */
  ZS_container_props_t props;
  ZSLoadCntrPropDefaults(&props);
  props.evicting = ZS_FALSE;
  props.persistent = ZS_TRUE;
  props.writethru = ZS_TRUE;
  props.size_kb = 0; /* transfer contianer size to KB */
  props.flash_only = ZS_TRUE;
	props.durability_level = ZS_DURABILITY_HW_CRASH_SAFE;

  ZS_cguid_t cguid = ZS_NULL_CGUID;
  rc = ZSOpenContainer(fdf_get_thd_state(), (char *)cid.c_str(), &props,
                        ZS_CTNR_CREATE|ZS_CTNR_RW_MODE,&cguid);

  if( rc != ZS_SUCCESS) {
    dout(10) << "FDF create collection failed :" << cid.c_str()
         << ", " << ZSStrError(rc) << dendl;
    assert(0);
  }

  dout(1) << "FDF create container(" << cid.c_str()
         << "), cguid: " << cguid << dendl;

  c.reset(new Collection(cguid));
  RWLock::WLocker l(coll_lock);
  coll_map.insert(hash_map<coll_t, CollectionRef>::value_type(cid, c));
  return 0;
}

int FDFStore::_destroy_collection(coll_t cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ZS_status_t rc;
  assert(fdf_get_thd_state());

  rc = ZSDeleteContainer(fdf_get_thd_state(), c->cguid);
  if( rc != ZS_SUCCESS) {
    dout(10) << "FDF delete collection failed :" << cid.c_str()
         << ", " << ZSStrError(rc) << dendl;
    assert(0);
  }

  RWLock::WLocker l(coll_lock);
  coll_map.erase(cid);
  return 0;
}

int FDFStore::_collection_add(coll_t cid, coll_t ocid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << ocid << " "
           << oid << " to be implemented" << dendl;
  return 0;
}

int FDFStore::_collection_move_rename(coll_t oldcid, const ghobject_t& oldoid,
				      coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << oldcid << " " << oldoid << " -> "
	   << cid << " " << oid << " to be implemented" << dendl;
  return 0;
}

int FDFStore::_collection_setattr(coll_t cid, const char *name,
				  const void *value, size_t size)
{
  dout(10) << __func__ << " " << cid << " " << name << dendl;

  ZS_status_t rc;

  CollectionRef c = fdf_get_container_for_coll(cid);

  string coll_attr = COLL_ATTR_PREFIX + cid.to_str() + COLL_ATTR_SEPARATOR + name;
  rc = ZSWriteObject(fdf_get_thd_state(), c->cguid,
                      (char *)coll_attr.c_str(), coll_attr.length(),
                      (char *)value, size, 0);
    if( rc != ZS_SUCCESS ) {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
          << c->cguid << ") set attr failed(key:" << name
          << " val:" << value << "), " << ZSStrError(rc) << dendl;
      return -1;
    }
  return 0;
}

int FDFStore::_collection_setattrs(coll_t cid, map<string,bufferptr> &aset)
{
  dout(10) << __func__ << " " << cid << dendl;

  ZS_status_t rc;
  CollectionRef c = fdf_get_container_for_coll(cid);

  string key, coll_attr;
  key = COLL_ATTR_PREFIX + cid.to_str() + COLL_ATTR_SEPARATOR;
  for (map<string,bufferptr>::const_iterator p = aset.begin();
       p != aset.end();
       ++p) {

    coll_attr = key + p->first;
    rc = ZSWriteObject(fdf_get_thd_state(), c->cguid,
                        (char *)coll_attr.c_str(), coll_attr.length(),
                        (char *)p->second.c_str(), p->second.length(), 0);
    if( rc != ZS_SUCCESS ) {
      dout(10) << "FDF container(" << cid.to_str() << "), cguid("
          << c->cguid << ") set attr failed(key:" << p->first
          << " val:" << p->second.c_str() << "), " << ZSStrError(rc) << dendl;
      assert(0);
    }

  }
  return 0;
}

int FDFStore::_collection_rmattr(coll_t cid, const char *name)
{
  dout(10) << __func__ << " " << cid << " " << name << dendl;

  ZS_status_t rc = ZS_SUCCESS;
  CollectionRef c = fdf_get_container_for_coll(cid);

  string coll_attr = COLL_ATTR_PREFIX + cid.to_str() + COLL_ATTR_SEPARATOR + name;
  rc = ZSDeleteObject(fdf_get_thd_state(), c->cguid,
                       (char *)coll_attr.c_str(), coll_attr.length());
  if( rc != ZS_SUCCESS ) {
    dout(10) << "FDF container(" << cid.to_str() << "), cguid("
         << c->cguid << ") " << __func__ << " failed(key:" << name
         << "), " << ZSStrError(rc) << dendl;
    return -1;
  }
  return 0;
}

int FDFStore::_collection_rename(const coll_t &cid, const coll_t &ncid)
{
  dout(10) << __func__ << " " << cid << " -> " << ncid
           << " not supported" << dendl;
  return 0;
}

int FDFStore::_split_collection(coll_t cid, uint32_t bits, uint32_t match,
				coll_t dest)
{
  dout(10) << __func__ << " " << cid << " " << bits << " " << match << " "
	   << dest << " not supported" << dendl;
  return 0;
}
