#ifndef IOTDB_FS_INDEX_MAINTENANCE_H
#define IOTDB_FS_INDEX_MAINTENANCE_H
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
/* Serial handles. JSON protocol iotdbfs-fs-index/2; caller owns returned strings.
 * NULL/-1 signals error; read the thread-local error before another API call.
 * Use fixed_native_result_free for every non-NULL returned JSON pointer.
 * Source arrays are caller-owned and must hold exactly 4096 values/mask bytes. */
void *fs_index_open_json(const char *config);
void fs_index_close(void *handle);
void *fs_index_publish_json(void *writer, const char *request);
void *fs_index_select_json(void *reader, const char *request);
void *fs_index_explain_json(void *reader, const char *request);
void *fs_index_status_json(void *handle);
void *fs_index_checkpoint_json(void *writer);
int fs_index_read_into(void *reader, const char *locator, float *values,
                       uint8_t *validity, uint64_t count);
/* Shared allocation/error boundary, also used by the frozen v1 interface. */
const char *fixed_native_last_error(void);
void fixed_native_result_free(void *result);
#ifdef __cplusplus
}
#endif
#endif
