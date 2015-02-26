
#ifndef BSDIFF_H_
#define BSDIFF_H_

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#include <sys/types.h>

int bsdiff_nocompress(u_char*, off_t, u_char*, off_t, u_char*, off_t, off_t*);
int bspatch_nocompress(u_char*, off_t, u_char*, off_t, u_char**, off_t*);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* BSDIFF_H_ */
