/*
 * Copyright (c) 2024, Gnosis Research Center, Illinois Institute of Technology
 * All rights reserved.
 *
 * This file is part of IOWarp Core.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef CLIO_BDEV_OBJECT_STORE_FACTORY_H_
#define CLIO_BDEV_OBJECT_STORE_FACTORY_H_

#include <memory>
#include <string>

#include "bdev_tasks.h"
#include "object_store_client.h"

namespace clio::run::bdev {

/**
 * Construct the cloud object-store client for a given bdev type + pool name.
 *
 * This is the factory seam that unifies the cloud backends: the runtime asks
 * for an ObjectStoreClient by BdevType (kS3/kGcs/kAzure/kAzurePage/kOss) and
 * gets back a concrete provider client with its config resolved from the pool
 * name and environment. Provider construction is the ONLY place that needs the
 * per-provider compile guards (CLIO_BDEV_*_ENABLED); a disabled provider's
 * case is preprocessed away and its header is never included, so the runtime
 * stays fully provider-agnostic.
 *
 * @param type The cloud bdev type to construct a client for.
 * @param pool_name The bdev pool name (e.g. "s3://bucket/prefix"); parsed by
 *                  the provider's FromEnvAndPoolName.
 * @return A new client, or nullptr if `type` is not a cloud type or its
 *         provider was not compiled in.
 */
std::unique_ptr<ObjectStoreClient> MakeObjectStoreClient(
    BdevType type, const std::string &pool_name);

}  // namespace clio::run::bdev

#endif  // CLIO_BDEV_OBJECT_STORE_FACTORY_H_
