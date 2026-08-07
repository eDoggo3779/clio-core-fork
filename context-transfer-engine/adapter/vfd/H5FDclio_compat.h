/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Platform layer for the CLIO HDF5 VFD.
 *
 * The driver keeps an authoritative native file on disk and performs ordinary
 * file I/O on it. That I/O is POSIX in H5FDclio.cc; this header supplies the
 * same operations on Windows, so the driver body reads identically on both
 * platforms and the port stays reviewable.
 *
 * Where Windows differs from POSIX in a way that matters, the choice here is
 * the one HDF5's own sec2 driver (H5FDsec2.c) makes:
 *
 *   file identity   MSVC reports st_ino == 0 for every file, so dev/ino cannot
 *                   tell two files apart. Identity comes from the volume serial
 *                   number plus the NTFS file index, as in H5FDsec2.c. cmp()
 *                   depends on this: if it says two opens of one file are
 *                   different files, HDF5 opens it twice with independent
 *                   metadata caches and corrupts it.
 *
 *   positional I/O  There is no pread/pwrite. ReadFile/WriteFile with an
 *                   OVERLAPPED offset are the real equivalent -- they do not
 *                   disturb the shared file pointer, so concurrent positional
 *                   reads stay correct. Seek-then-read would not.
 *
 *   binary mode     _O_BINARY is not optional. Without it the CRT rewrites
 *                   \n as \r\n on the way out, which corrupts an HDF5 file
 *                   silently and unrecoverably.
 *
 *   locking         flock() has no Windows equivalent; LockFileEx over the
 *                   whole range is the standard stand-in, with
 *                   LOCKFILE_FAIL_IMMEDIATELY for flock's LOCK_NB.
 *
 *   errno           Win32 reports through GetLastError(), but the driver's
 *                   error macro prints errno. Every failure path below sets
 *                   errno so those messages stay meaningful.
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef CLIO_CTE_ADAPTER_VFD_H5FDCLIO_COMPAT_H_
#define CLIO_CTE_ADAPTER_VFD_H5FDCLIO_COMPAT_H_

#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

#include <fcntl.h>
#include <io.h>
#include <share.h>
#else
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#endif

/* 64-bit file offset on every platform. MSVC's off_t is 32 bits, which would
 * silently cap the driver's usable address space -- and MAXADDR, which is
 * derived from sizeof() this type -- at 2 GiB. */
typedef int64_t clio_vfd_off_t;
typedef int64_t clio_vfd_ssize_t;

/* Filesystem identity of the authoritative native file; see the note above. */
typedef struct clio_vfd_file_id_t {
#ifdef _WIN32
  DWORD volume_serial;
  DWORD index_high;
  DWORD index_low;
#else
  dev_t dev;
  ino_t ino;
#endif
} clio_vfd_file_id_t;

#ifdef _WIN32

/* Win32 failures are reported through GetLastError(); the driver prints errno.
 * Map the cases the driver can actually distinguish and fall back to EIO. */
static inline void clio_vfd_set_errno_from_win32(DWORD err) {
  switch (err) {
    case ERROR_SUCCESS:
      errno = 0;
      break;
    case ERROR_FILE_NOT_FOUND:
    case ERROR_PATH_NOT_FOUND:
      errno = ENOENT;
      break;
    case ERROR_ACCESS_DENIED:
      errno = EACCES;
      break;
    case ERROR_FILE_EXISTS:
    case ERROR_ALREADY_EXISTS:
      errno = EEXIST;
      break;
    /* Another handle holds the lock. flock(LOCK_NB) reports this as
     * EWOULDBLOCK, and the driver names lock contention specifically. */
    case ERROR_LOCK_VIOLATION:
    case ERROR_SHARING_VIOLATION:
      errno = EWOULDBLOCK;
      break;
    case ERROR_DISK_FULL:
      errno = ENOSPC;
      break;
    case ERROR_INVALID_HANDLE:
      errno = EBADF;
      break;
    case ERROR_NOT_SUPPORTED:
      errno = ENOSYS;
      break;
    default:
      errno = EIO;
      break;
  }
}

static inline HANDLE clio_vfd_handle(int fd) {
  HANDLE h = (HANDLE)_get_osfhandle(fd);
  if (h == INVALID_HANDLE_VALUE) {
    errno = EBADF;
  }
  return h;
}

#endif /* _WIN32 */

static inline int clio_vfd_open(const char *path, int o_flags, int mode) {
#ifdef _WIN32
  /* _SH_DENYNO keeps the file readable by other tools while it is open, which
   * is what open(2) does and what the driver's h5dump-while-writing story
   * depends on. _O_BINARY is mandatory -- see the header note. */
  int fd = -1;
  errno_t e = _sopen_s(&fd, path, o_flags | _O_BINARY, _SH_DENYNO, mode);
  if (e != 0) {
    errno = e;
    return -1;
  }
  return fd;
#else
  return open(path, o_flags, mode);
#endif
}

static inline int clio_vfd_close(int fd) {
#ifdef _WIN32
  return _close(fd);
#else
  return close(fd);
#endif
}

static inline clio_vfd_ssize_t clio_vfd_pread(int fd, void *buf, size_t count,
                                              clio_vfd_off_t off) {
#ifdef _WIN32
  HANDLE h = clio_vfd_handle(fd);
  if (h == INVALID_HANDLE_VALUE) {
    return -1;
  }
  DWORD want = (count > 0x7fffffffu) ? 0x7fffffffu : (DWORD)count;
  OVERLAPPED ov;
  memset(&ov, 0, sizeof(ov));
  LARGE_INTEGER li;
  li.QuadPart = off;
  ov.Offset = li.LowPart;
  ov.OffsetHigh = (DWORD)li.HighPart;
  DWORD got = 0;
  if (!ReadFile(h, buf, want, &got, &ov)) {
    DWORD e = GetLastError();
    /* Reading at or past end of file: pread reports that as 0, and the driver
     * zero-fills the remainder on exactly that signal. */
    if (e == ERROR_HANDLE_EOF) {
      return 0;
    }
    clio_vfd_set_errno_from_win32(e);
    return -1;
  }
  return (clio_vfd_ssize_t)got;
#else
  return (clio_vfd_ssize_t)pread(fd, buf, count, (off_t)off);
#endif
}

static inline clio_vfd_ssize_t clio_vfd_pwrite(int fd, const void *buf,
                                               size_t count,
                                               clio_vfd_off_t off) {
#ifdef _WIN32
  HANDLE h = clio_vfd_handle(fd);
  if (h == INVALID_HANDLE_VALUE) {
    return -1;
  }
  DWORD want = (count > 0x7fffffffu) ? 0x7fffffffu : (DWORD)count;
  OVERLAPPED ov;
  memset(&ov, 0, sizeof(ov));
  LARGE_INTEGER li;
  li.QuadPart = off;
  ov.Offset = li.LowPart;
  ov.OffsetHigh = (DWORD)li.HighPart;
  DWORD put = 0;
  if (!WriteFile(h, buf, want, &put, &ov)) {
    clio_vfd_set_errno_from_win32(GetLastError());
    return -1;
  }
  return (clio_vfd_ssize_t)put;
#else
  return (clio_vfd_ssize_t)pwrite(fd, buf, count, (off_t)off);
#endif
}

static inline int clio_vfd_ftruncate(int fd, clio_vfd_off_t length) {
#ifdef _WIN32
  /* _chsize_s takes the 64-bit length directly and returns an errno value
   * rather than setting it. */
  errno_t e = _chsize_s(fd, (__int64)length);
  if (e != 0) {
    errno = e;
    return -1;
  }
  return 0;
#else
  return ftruncate(fd, (off_t)length);
#endif
}

static inline int clio_vfd_fsync(int fd) {
#ifdef _WIN32
  return _commit(fd);
#else
  return fsync(fd);
#endif
}

/* Non-blocking advisory whole-file lock. Returns 0, or -1 with errno set;
 * EWOULDBLOCK means another handle holds it. */
static inline int clio_vfd_lock(int fd, int exclusive) {
#ifdef _WIN32
  HANDLE h = clio_vfd_handle(fd);
  if (h == INVALID_HANDLE_VALUE) {
    return -1;
  }
  DWORD flags = LOCKFILE_FAIL_IMMEDIATELY;
  if (exclusive) {
    flags |= LOCKFILE_EXCLUSIVE_LOCK;
  }
  OVERLAPPED ov;
  memset(&ov, 0, sizeof(ov));
  if (!LockFileEx(h, flags, 0, MAXDWORD, MAXDWORD, &ov)) {
    clio_vfd_set_errno_from_win32(GetLastError());
    return -1;
  }
  return 0;
#else
  int op = (exclusive ? LOCK_EX : LOCK_SH) | LOCK_NB;
  return flock(fd, op);
#endif
}

static inline int clio_vfd_unlock(int fd) {
#ifdef _WIN32
  HANDLE h = clio_vfd_handle(fd);
  if (h == INVALID_HANDLE_VALUE) {
    return -1;
  }
  OVERLAPPED ov;
  memset(&ov, 0, sizeof(ov));
  if (!UnlockFileEx(h, 0, MAXDWORD, MAXDWORD, &ov)) {
    DWORD e = GetLastError();
    /* Unlocking a range that is not locked is not an error here: the driver
     * calls unlock unconditionally on paths where the lock may never have been
     * taken, and flock(LOCK_UN) is equally forgiving. */
    if (e == ERROR_NOT_LOCKED) {
      return 0;
    }
    clio_vfd_set_errno_from_win32(e);
    return -1;
  }
  return 0;
#else
  return flock(fd, LOCK_UN);
#endif
}

/* One call for both things the driver needs at open: the identity cmp() will
 * compare, and the current size it uses as EOF. */
static inline int clio_vfd_fstat(int fd, clio_vfd_file_id_t *id,
                                 clio_vfd_off_t *size) {
#ifdef _WIN32
  HANDLE h = clio_vfd_handle(fd);
  if (h == INVALID_HANDLE_VALUE) {
    return -1;
  }
  BY_HANDLE_FILE_INFORMATION info;
  if (!GetFileInformationByHandle(h, &info)) {
    clio_vfd_set_errno_from_win32(GetLastError());
    return -1;
  }
  id->volume_serial = info.dwVolumeSerialNumber;
  id->index_high = info.nFileIndexHigh;
  id->index_low = info.nFileIndexLow;
  LARGE_INTEGER li;
  li.LowPart = info.nFileSizeLow;
  li.HighPart = (LONG)info.nFileSizeHigh;
  *size = (clio_vfd_off_t)li.QuadPart;
  return 0;
#else
  struct stat st;
  if (fstat(fd, &st) < 0) {
    return -1;
  }
  id->dev = st.st_dev;
  id->ino = st.st_ino;
  *size = (clio_vfd_off_t)st.st_size;
  return 0;
#endif
}

/* Ordering comparison for cmp(): -1 / 0 / 1. */
static inline int clio_vfd_cmp_file_id(const clio_vfd_file_id_t *a,
                                       const clio_vfd_file_id_t *b) {
#ifdef _WIN32
  if (a->volume_serial < b->volume_serial) return -1;
  if (a->volume_serial > b->volume_serial) return 1;
  if (a->index_high < b->index_high) return -1;
  if (a->index_high > b->index_high) return 1;
  if (a->index_low < b->index_low) return -1;
  if (a->index_low > b->index_low) return 1;
#else
  if (a->dev < b->dev) return -1;
  if (a->dev > b->dev) return 1;
  if (a->ino < b->ino) return -1;
  if (a->ino > b->ino) return 1;
#endif
  return 0;
}

static inline int clio_vfd_unlink(const char *path) {
#ifdef _WIN32
  return _unlink(path);
#else
  return unlink(path);
#endif
}

#endif /* CLIO_CTE_ADAPTER_VFD_H5FDCLIO_COMPAT_H_ */
