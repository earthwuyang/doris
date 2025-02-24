// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "cloud/cloud_internal_service.h"

#include "cloud/cloud_storage_engine.h"

namespace doris {

CloudInternalServiceImpl::CloudInternalServiceImpl(CloudStorageEngine& engine, ExecEnv* exec_env)
        : PInternalService(exec_env), _engine(engine) {}

CloudInternalServiceImpl::~CloudInternalServiceImpl() = default;

void CloudInternalServiceImpl::alter_vault_sync(google::protobuf::RpcController* controller,
                                                const doris::PAlterVaultSyncRequest* request,
                                                PAlterVaultSyncResponse* response,
                                                google::protobuf::Closure* done) {
    LOG(INFO) << "alter be to sync vault info from Meta Service";
    _engine.sync_storage_vault();
}

} // namespace doris
