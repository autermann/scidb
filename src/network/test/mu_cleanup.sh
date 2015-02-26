#!/bin/bash

iquery -aq "list()"

iquery -aq "remove(XXX_MU_XXX_SCHEMA)" 1>/dev/null 2>/dev/null
iquery -aq "remove(XXX_MU_XXX_ORIG)" 1>/dev/null 2>/dev/null
iquery -aq "remove(XXX_MU_XXX_COPY)" 1>/dev/null 2>/dev/null
iquery -aq "remove(XXX_MU_XXX_M1024x1024)" 1>/dev/null 2>/dev/null
exit 0

