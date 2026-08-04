// Minimal C shim over Apple's unified logging.
//
// `os_log_with_type` is a macro, not a function: at the call site it plants the
// format string in the calling image's __TEXT section and hands `_os_log_impl`
// a `__dso_handle` plus a hand-packed argument buffer. Reproducing that from
// Rust means open-coding an undocumented buffer layout and guessing which image
// the dso handle belongs to, so the single call site lives here instead and
// Rust only ever passes a pre-formatted, NUL-terminated UTF-8 string.
//
// `%{public}s` is required: dynamic strings in unified logging are redacted to
// `<private>` unless explicitly marked public.
//
// Compiled by build.rs for Apple targets only.

#include <os/log.h>
#include <stdint.h>

void *jazz_rn_os_log_create(const char *subsystem, const char *category) {
  return (void *)os_log_create(subsystem, category);
}

void jazz_rn_os_log_emit(void *log, uint8_t type, const char *message) {
  os_log_with_type((os_log_t)log, (os_log_type_t)type, "%{public}s", message);
}
