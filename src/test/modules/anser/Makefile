#-------------------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# src/test/modules/anser/Makefile
#
#-------------------------------------------------------------------------

MODULE_big = test_anser
OBJS = \
	$(WIN32RES) \
	anser_test.o
PGFILEDESC = "test_anser - test helpers for Anser"

# anser_test.c drives loopback libpq connections for the client/multi-consumer
# round-trip tests.
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)
SHLIB_PREREQS = submake-libpq

EXTENSION = test_anser
DATA = test_anser--1.0.sql

REGRESS = test_anser anser_runtime_filter

# CI runs a single make target per test config, and pg_regress can only CREATE
# EXTENSION test_anser once the module is installed (src/test/modules is not
# covered by the top-level install).  One-shot target so the CI matrix needs
# only one entry whose log still contains the regression results.
.PHONY: installcheck-with-install
installcheck-with-install:
	$(MAKE) install
	$(MAKE) installcheck

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/anser
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
