#!/bin/bash

iquery -aq "remove(XXX_MU_XXX_SCHEMA)" 1>/dev/null 2>/dev/null
iquery -aq "remove(XXX_MU_XXX_ORIG)" 1>/dev/null 2>/dev/null
iquery -aq "remove(XXX_MU_XXX_COPY)" 1>/dev/null 2>/dev/null
iquery -aq "remove(XXX_MU_XXX_M1024x1024)" 1>/dev/null 2>/dev/null

iquery -naq "CREATE ARRAY XXX_MU_XXX_SCHEMA < price : double > [ equity=1:300,10,0, time=1:300,10,0 ]"
iquery -naq "store(build(XXX_MU_XXX_SCHEMA, equity*time*7.0), XXX_MU_XXX_ORIG)"
iquery -naq "store(XXX_MU_XXX_ORIG,XXX_MU_XXX_COPY)"
iquery -naq "load_library('dense_linear_algebra')"
iquery -naq "create array XXX_MU_XXX_M1024x1024 <x:double>[i=0:1024-1,32,0, j=0:1024-1,32,0]"
iquery -naq "store(build(XXX_MU_XXX_M1024x1024, iif(i=j,1,0)), XXX_MU_XXX_M1024x1024)"

