#!/bin/bash

iquery -aq "list()"

#iquery -aq "gemm(XXX_MU_XXX_M1024x1024, build(XXX_MU_XXX_M1024x1024, 0), build(XXX_MU_XXX_M1024x1024, 1))" 1>/dev/null
iquery -aq "filter(XXX_MU_XXX_COPY, equity=1)" 1>/dev/null
iquery -aq "between(XXX_MU_XXX_COPY, null,null,null,null)" 1>/dev/null
iquery -aq "between(XXX_MU_XXX_COPY, 1,0,null,null)" 1>/dev/null
iquery -aq "between(XXX_MU_XXX_COPY, 0,0,null,null)" 1>/dev/null
iquery -aq "join(XXX_MU_XXX_COPY,XXX_MU_XXX_COPY)" 1>/dev/null
#iquery -aq "gesvd(XXX_MU_XXX_M1024x1024, 'values')" 1>/dev/null
