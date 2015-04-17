// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef RADOS_IMPORT_H_
#define RADOS_IMPORT_H_

#include <string>

#include "include/rados/librados.hpp"
#include "include/buffer.h"

#include "RadosDump.h"

/**
 * Specialization of RadosDump that adds
 * methods for importing objects from a stream
 * to a live cluster.
 */
class RadosImport : public RadosDump
{
  protected:
    uint64_t align;
    int get_object_rados(librados::IoCtx &ioctx, bufferlist &bl);

  public:
    RadosImport(int file_fd_, uint64_t align_)
      : RadosDump(file_fd_), align(align_)
    {}

    int import(std::string pool);
    int import(librados::IoCtx &io_ctx);
};

#endif // RADOS_IMPORT_H_

