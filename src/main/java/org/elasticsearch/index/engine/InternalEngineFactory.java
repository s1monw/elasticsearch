/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.engine;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.translog.Translog;

public class InternalEngineFactory implements EngineFactory {
    @Override
    public Engine newReadWriteEngine(EngineConfig config, boolean skipTranslogRecovery) {
        if (IndexMetaData.isOnSharedFilesystem(config.getIndexSettings())) {
          return new InternalEngine(config, skipTranslogRecovery) {
              @Override
              public void recover(RecoveryHandler recoveryHandler) throws EngineException {
                  store.incRef();
                  try  {
                      try (ReleasableLock lock = writeLock.acquire()) {
                          // phase1 under lock
                          ensureOpen();
                          try {
                              recoveryHandler.phase1(null);
                          } catch (Throwable e) {
                              maybeFailEngine("recovery phase 1", e);
                              throw new RecoveryEngineException(shardId, 1, "Execution failed", wrapIfClosed(e));
                          }
                      }
                      try {
                          recoveryHandler.phase2(null);
                      } catch (Throwable e) {
                          maybeFailEngine("recovery phase 2", e);
                          throw new RecoveryEngineException(shardId, 2, "Execution failed", wrapIfClosed(e));
                      }
                      try {
                          recoveryHandler.phase3(null);
                      } catch (Throwable e) {
                          maybeFailEngine("recovery phase 3", e);
                          throw new RecoveryEngineException(shardId, 3, "Execution failed", wrapIfClosed(e));
                      }
                  } finally {
                      store.decRef();
                  }
              }
          };
        }
        return new InternalEngine(config, skipTranslogRecovery);
    }

    @Override
    public Engine newReadOnlyEngine(EngineConfig config) {
        return new ShadowEngine(config);
    }
}
