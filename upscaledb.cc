/*
 * upscaledb.cc — supports multiple lock backends via compile-time define:
 *   -DLOCK_BOOST_MUTEX   boost::mutex  (upscaledb native default)
 *   -DLOCK_PTHREAD_MUTEX pthread_mutex_t
 *   -DLOCK_PTHREAD_SPIN  pthread_spinlock_t
 *   -DLOCK_TICKET        custom ticket lock (FIFO)
 *   -DHFAIRLOCK          H-SCL hierarchical fair lock
 * Default (no define) = LOCK_BOOST_MUTEX
 */

#include <iostream>
#include <boost/filesystem.hpp>
#include <ups/upscaledb_int.h>
#include "metrics.h"
#include "configuration.h"
#include "misc.h"
#include "upscaledb.h"
#include "1globals/globals.h"

// ── Lock backend ──────────────────────────────────────────────────────────────
#if defined(HFAIRLOCK)
extern "C" {
  #include "locks/hfairlock.h"
  #include "rdtsc.h"
  #include "hscl_common.h"
}
hfairlock_t ms_hfairlock;
int         g_nthreads  = 0;
node_t     *g_hierarchy = NULL;
#define LOCK_ACQUIRE() hfairlock_acquire(&ms_hfairlock)
#define LOCK_RELEASE() hfairlock_release(&ms_hfairlock)

#elif defined(LOCK_PTHREAD_MUTEX)
#include <pthread.h>
static pthread_mutex_t ms_pmutex = PTHREAD_MUTEX_INITIALIZER;
#define LOCK_ACQUIRE() pthread_mutex_lock(&ms_pmutex)
#define LOCK_RELEASE() pthread_mutex_unlock(&ms_pmutex)

#elif defined(LOCK_PTHREAD_SPIN)
#include <pthread.h>
static pthread_spinlock_t ms_pspin;
static int ms_pspin_inited = 0;
#define LOCK_ACQUIRE() pthread_spin_lock(&ms_pspin)
#define LOCK_RELEASE() pthread_spin_unlock(&ms_pspin)

#elif defined(LOCK_TICKET)
extern "C" {
#include "locks/ticket_lock.h"
}
static ticket_lock_t ms_ticket;
#define LOCK_ACQUIRE() ticket_lock_acquire(&ms_ticket)
#define LOCK_RELEASE() ticket_lock_release(&ms_ticket)

#else
// Default: boost::mutex (original)
#define LOCK_BOOST_MUTEX
#define LOCK_ACQUIRE() ScopedLock _sl_(ms_mutex)
#define LOCK_RELEASE() (void)0
#endif
// ─────────────────────────────────────────────────────────────────────────────

ups_env_t *UpscaleDatabase::ms_env = 0;
#ifdef UPS_ENABLE_REMOTE
ups_env_t *UpscaleDatabase::ms_remote_env = 0;
ups_srv_t *UpscaleDatabase::ms_srv = 0;
#endif
#if defined(LOCK_BOOST_MUTEX) || (!defined(HFAIRLOCK) && !defined(LOCK_PTHREAD_MUTEX) && !defined(LOCK_PTHREAD_SPIN) && !defined(LOCK_TICKET))
Mutex UpscaleDatabase::ms_mutex;
#endif
int UpscaleDatabase::ms_refcount;

void init_bench_lock(int nthreads) {
#if defined(LOCK_PTHREAD_SPIN)
    if (!ms_pspin_inited) { pthread_spin_init(&ms_pspin, PTHREAD_PROCESS_PRIVATE); ms_pspin_inited=1; }
#elif defined(LOCK_TICKET)
    ticket_lock_init(&ms_ticket);
#else
    (void)nthreads;
#endif
}

static int compare_keys(ups_db_t *db,
    const uint8_t *ld, uint32_t ls, const uint8_t *rd, uint32_t rs) {
  (void)db;
  if (ls < rs) { int m=::memcmp(ld,rd,ls); return m<0?-1:m>0?+1:-1; }
  if (rs < ls) { int m=::memcmp(ld,rd,rs); return m<0?-1:m>0?+1:+1; }
  int m=memcmp(ld,rd,ls); return m<0?-1:m>0?+1:0;
}

ups_status_t UpscaleDatabase::do_create_env() {
  ups_status_t st = 0; uint32_t flags = 0;
  ups_parameter_t params[6] = {{0,0}};
  LOCK_ACQUIRE();
  ms_refcount++;
  upscaledb::Globals::ms_extended_threshold  = m_config->extkey_threshold;
  upscaledb::Globals::ms_duplicate_threshold = m_config->duptable_threshold;
  int p=0;
  if (ms_env == 0) {
    params[p].name=UPS_PARAM_CACHE_SIZE;    params[p].value=m_config->cachesize;     p++;
    params[p].name=UPS_PARAM_PAGE_SIZE;     params[p].value=m_config->pagesize;      p++;
    params[p].name=UPS_PARAM_POSIX_FADVISE; params[p].value=m_config->posix_fadvice; p++;
    if (m_config->use_encryption)       { params[p].name=UPS_PARAM_ENCRYPTION_KEY;    params[p].value=(uint64_t)"1234567890123456"; p++; }
    if (m_config->journal_compression)  { params[p].name=UPS_PARAM_JOURNAL_COMPRESSION; params[p].value=m_config->journal_compression; p++; }
    flags |= m_config->inmemory             ? UPS_IN_MEMORY                      : 0;
    flags |= m_config->no_mmap              ? UPS_DISABLE_MMAP                   : 0;
    flags |= m_config->cacheunlimited       ? UPS_CACHE_UNLIMITED                : 0;
    flags |= m_config->use_transactions     ? UPS_ENABLE_TRANSACTIONS            : 0;
    flags |= m_config->flush_txn_immediately? UPS_FLUSH_TRANSACTIONS_IMMEDIATELY : 0;
    flags |= m_config->use_fsync            ? UPS_ENABLE_FSYNC                   : 0;
    flags |= m_config->disable_recovery     ? UPS_DISABLE_RECOVERY               : 0;
    flags |= m_config->enable_crc32         ? UPS_ENABLE_CRC32                   : 0;
    boost::filesystem::remove("test-ham.db");
    st = ups_env_create(&ms_env,"test-ham.db",flags,0664,&params[0]);
    if (st) { LOG_ERROR(("ups_env_create failed %d (%s)\n",st,ups_strerror(st))); LOCK_RELEASE(); return st; }
  }
  LOCK_RELEASE(); return st;
}

ups_status_t UpscaleDatabase::do_open_env() {
  ups_status_t st=0; uint32_t flags=0;
  ups_parameter_t params[6]={{0,0}};
  LOCK_ACQUIRE();
  ms_refcount++;
  upscaledb::Globals::ms_extended_threshold  = m_config->extkey_threshold;
  upscaledb::Globals::ms_duplicate_threshold = m_config->duptable_threshold;
  if (ms_env==0) {
    int p=0;
    params[p].name=UPS_PARAM_CACHE_SIZE;    params[p].value=m_config->cachesize;     p++;
    params[p].name=UPS_PARAM_POSIX_FADVISE; params[p].value=m_config->posix_fadvice; p++;
    if (m_config->use_encryption) { params[p].name=UPS_PARAM_ENCRYPTION_KEY; params[p].value=(uint64_t)"1234567890123456"; p++; }
    flags |= m_config->no_mmap           ? UPS_DISABLE_MMAP                          : 0;
    flags |= m_config->cacheunlimited    ? UPS_CACHE_UNLIMITED                       : 0;
    flags |= m_config->use_transactions  ? (UPS_ENABLE_TRANSACTIONS|UPS_AUTO_RECOVERY): 0;
    flags |= m_config->flush_txn_immediately?UPS_FLUSH_TRANSACTIONS_IMMEDIATELY      : 0;
    flags |= m_config->use_fsync         ? UPS_ENABLE_FSYNC                          : 0;
    flags |= m_config->disable_recovery  ? UPS_DISABLE_RECOVERY                      : 0;
    flags |= m_config->read_only         ? UPS_READ_ONLY                             : 0;
    flags |= m_config->enable_crc32      ? UPS_ENABLE_CRC32                          : 0;
    st=ups_env_open(&ms_env,"test-ham.db",flags,&params[0]);
    if (st) { LOG_ERROR(("ups_env_open failed %d (%s)\n",st,ups_strerror(st))); LOCK_RELEASE(); return st; }
  }
  LOCK_RELEASE(); return st;
}

ups_status_t UpscaleDatabase::do_close_env() {
  LOCK_ACQUIRE();
  if (m_env) ups_env_get_metrics(m_env,&m_upscaledb_metrics);
  if (ms_refcount==0) { assert(m_env==0); assert(ms_env==0); LOCK_RELEASE(); return 0; }
  if (--ms_refcount>0) { LOCK_RELEASE(); return 0; }
  if (m_env)  { ups_env_close(m_env,0);  m_env=0; }
  if (ms_env) { ups_env_get_metrics(ms_env,&m_upscaledb_metrics); ups_env_close(ms_env,0); ms_env=0; }
  LOCK_RELEASE(); return 0;
}

ups_status_t UpscaleDatabase::do_create_db(int id) {
  ups_status_t st; ups_parameter_t params[8]={{0,0}}; int n=0;
  params[n].name=UPS_PARAM_KEY_SIZE; params[n].value=0; n++;
  switch(m_config->key_type) {
    case Configuration::kKeyCustom:
      params[0].value=m_config->key_is_fixed_size?m_config->key_size:UPS_KEY_SIZE_UNLIMITED;
      params[n].name=UPS_PARAM_KEY_TYPE; params[n].value=UPS_TYPE_CUSTOM; n++; break;
    case Configuration::kKeyBinary: case Configuration::kKeyString:
      params[0].value=m_config->key_is_fixed_size?m_config->key_size:UPS_KEY_SIZE_UNLIMITED; break;
    case Configuration::kKeyUint8:  params[n].name=UPS_PARAM_KEY_TYPE; params[n].value=UPS_TYPE_UINT8;  n++; break;
    case Configuration::kKeyUint16: params[n].name=UPS_PARAM_KEY_TYPE; params[n].value=UPS_TYPE_UINT16; n++; break;
    case Configuration::kKeyUint32: params[n].name=UPS_PARAM_KEY_TYPE; params[n].value=UPS_TYPE_UINT32; n++; break;
    case Configuration::kKeyUint64: params[n].name=UPS_PARAM_KEY_TYPE; params[n].value=UPS_TYPE_UINT64; n++; break;
    case Configuration::kKeyReal32: params[n].name=UPS_PARAM_KEY_TYPE; params[n].value=UPS_TYPE_REAL32; n++; break;
    case Configuration::kKeyReal64: params[n].name=UPS_PARAM_KEY_TYPE; params[n].value=UPS_TYPE_REAL64; n++; break;
    default: assert(!"shouldn't be here");
  }
  switch(m_config->record_type) {
    case Configuration::kKeyBinary: case Configuration::kKeyString: break;
    case Configuration::kKeyUint8:  params[n].name=UPS_PARAM_RECORD_TYPE; params[n].value=UPS_TYPE_UINT8;  n++; break;
    case Configuration::kKeyUint16: params[n].name=UPS_PARAM_RECORD_TYPE; params[n].value=UPS_TYPE_UINT16; n++; break;
    case Configuration::kKeyUint32: params[n].name=UPS_PARAM_RECORD_TYPE; params[n].value=UPS_TYPE_UINT32; n++; break;
    case Configuration::kKeyUint64: params[n].name=UPS_PARAM_RECORD_TYPE; params[n].value=UPS_TYPE_UINT64; n++; break;
    case Configuration::kKeyReal32: params[n].name=UPS_PARAM_RECORD_TYPE; params[n].value=UPS_TYPE_REAL32; n++; break;
    case Configuration::kKeyReal64: params[n].name=UPS_PARAM_RECORD_TYPE; params[n].value=UPS_TYPE_REAL64; n++; break;
    default: assert(!"shouldn't be here");
  }
  params[n].name=UPS_PARAM_RECORD_SIZE; params[n].value=m_config->rec_size_fixed; n++;
  if(m_config->record_compression){params[n].name=UPS_PARAM_RECORD_COMPRESSION;params[n].value=m_config->record_compression;n++;}
  if(m_config->key_compression)   {params[n].name=UPS_PARAM_KEY_COMPRESSION;    params[n].value=m_config->key_compression;   n++;}
  if(m_config->key_type==Configuration::kKeyCustom){ups_register_compare("cmp",compare_keys);params[n].name=UPS_PARAM_CUSTOM_COMPARE_NAME;params[n].value=(uint64_t)"cmp";n++;}
  uint32_t flags=0;
  flags|=m_config->duplicate?UPS_ENABLE_DUPLICATES:0;
  flags|=m_config->record_number32?UPS_RECORD_NUMBER32:0;
  flags|=m_config->record_number64?UPS_RECORD_NUMBER64:0;
  if(m_config->force_records_inline) flags|=UPS_FORCE_RECORDS_INLINE;
  st=ups_env_create_db(m_env?m_env:ms_env,&m_db,1+id,flags,&params[0]);
  if(st){LOG_ERROR(("ups_env_create_db failed %d (%s)\n",st,ups_strerror(st)));exit(-1);}
  return 0;
}

ups_status_t UpscaleDatabase::do_open_db(int id) {
  ups_parameter_t params[6]={{0,0}};
  ups_register_compare("cmp",compare_keys);
  ups_status_t st=ups_env_open_db(m_env?m_env:ms_env,&m_db,1+id,0,&params[0]);
  if(st){LOG_ERROR(("ups_env_open_db failed %d (%s)\n",st,ups_strerror(st)));exit(-1);}
  return st;
}
ups_status_t UpscaleDatabase::do_close_db(){if(m_db)ups_db_close(m_db,UPS_AUTO_CLEANUP);m_db=0;return 0;}
ups_status_t UpscaleDatabase::do_flush(){return ups_env_flush(m_env?m_env:ms_env,0);}
ups_status_t UpscaleDatabase::do_insert(Txn*txn,ups_key_t*key,ups_record_t*record){
  uint32_t f=0; if(m_config->overwrite)f|=UPS_OVERWRITE; else if(m_config->duplicate)f|=UPS_DUPLICATE;
  ups_key_t rk={0}; if(m_config->record_number32||m_config->record_number64)key=&rk;
  ups_status_t st=ups_db_insert(m_db,(ups_txn_t*)txn,key,record,f);
  if(st)LOG_VERBOSE(("insert failed %d\n",st)); return st;}
ups_status_t UpscaleDatabase::do_erase(Txn*txn,ups_key_t*key){
  ups_status_t st=ups_db_erase(m_db,(ups_txn_t*)txn,key,0);
  if(st)LOG_VERBOSE(("erase failed %d\n",st)); return st;}
ups_status_t UpscaleDatabase::do_find(Txn*txn,ups_key_t*key,ups_record_t*record){
  ups_status_t st=ups_db_find(m_db,(ups_txn_t*)txn,key,record,0);
  if(st)LOG_VERBOSE(("find failed %d\n",st)); return st;}
ups_status_t UpscaleDatabase::do_check_integrity(){return ups_db_check_integrity(m_db,0);}
Database::Txn*UpscaleDatabase::do_txn_begin(){
  ups_status_t st=ups_txn_begin(&m_txn,m_env?m_env:ms_env,0,0,0);
  if(st){LOG_ERROR(("txn_begin failed %d\n",st));return 0;} return(Database::Txn*)m_txn;}
ups_status_t UpscaleDatabase::do_txn_commit(Txn*txn){
  assert((ups_txn_t*)txn==m_txn);
  ups_status_t st=ups_txn_commit((ups_txn_t*)txn,0);
  if(st)LOG_ERROR(("txn_commit failed %d\n",st)); m_txn=0; return st;}
ups_status_t UpscaleDatabase::do_txn_abort(Txn*txn){
  assert((ups_txn_t*)txn==m_txn);
  ups_status_t st=ups_txn_abort((ups_txn_t*)txn,0);
  if(st)LOG_ERROR(("txn_abort failed %d\n",st)); m_txn=0; return st;}
Database::Cursor*UpscaleDatabase::do_cursor_create(){
  ups_cursor_t*c; ups_status_t st=ups_cursor_create(&c,m_db,m_txn,0);
  if(st){LOG_ERROR(("cursor_create failed %d\n",st));exit(-1);} return(Database::Cursor*)c;}
ups_status_t UpscaleDatabase::do_cursor_insert(Cursor*cursor,ups_key_t*key,ups_record_t*record){
  uint32_t f=0; if(m_config->overwrite)f|=UPS_OVERWRITE;
  if(m_config->duplicate==Configuration::kDuplicateFirst)f|=UPS_DUPLICATE|UPS_DUPLICATE_INSERT_FIRST;
  else if(m_config->duplicate==Configuration::kDuplicateLast)f|=UPS_DUPLICATE|UPS_DUPLICATE_INSERT_LAST;
  ups_status_t st=ups_cursor_insert((ups_cursor_t*)cursor,key,record,f);
  if(st)LOG_VERBOSE(("cursor_insert failed %d\n",st)); return st;}
ups_status_t UpscaleDatabase::do_cursor_erase(Cursor*cursor,ups_key_t*key){
  ups_status_t st=ups_cursor_find((ups_cursor_t*)cursor,key,0,0);
  if(st){LOG_VERBOSE(("cursor_find failed %d\n",st));return st;}
  st=ups_cursor_erase((ups_cursor_t*)cursor,0);
  if(st)LOG_VERBOSE(("cursor_erase failed %d\n",st)); return st;}
ups_status_t UpscaleDatabase::do_cursor_find(Cursor*cursor,ups_key_t*key,ups_record_t*record){
  ups_status_t st=ups_cursor_find((ups_cursor_t*)cursor,key,record,0);
  if(st)LOG_VERBOSE(("cursor_find failed %d\n",st)); return st;}
ups_status_t UpscaleDatabase::do_cursor_get_previous(Cursor*cursor,ups_key_t*key,ups_record_t*record,bool sd){
  return ups_cursor_move((ups_cursor_t*)cursor,key,record,UPS_CURSOR_PREVIOUS|(sd?UPS_SKIP_DUPLICATES:0));}
ups_status_t UpscaleDatabase::do_cursor_get_next(Cursor*cursor,ups_key_t*key,ups_record_t*record,bool sd){
  return ups_cursor_move((ups_cursor_t*)cursor,key,record,UPS_CURSOR_NEXT|(sd?UPS_SKIP_DUPLICATES:0));}
ups_status_t UpscaleDatabase::do_cursor_close(Cursor*cursor){return ups_cursor_close((ups_cursor_t*)cursor);}