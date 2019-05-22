/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;

public class RepositoryIndexStore extends IndexStore{

    public static Setting<String> REPOSITORY_NAME = Setting.simpleString("index.store.repsitory.name", Setting.Property.IndexScope);
    public static Setting<String> SNAPSHOT_NAME = Setting.simpleString("index.store.snapshot.name", Setting.Property.IndexScope);
    public static Setting<String> SNAPSHOT_UUID = Setting.simpleString("index.store.snapshot.uuid", Setting.Property.IndexScope);
    private final BlobStoreRepository repository;
    private final SnapshotId snapshotId;

    public RepositoryIndexStore(IndexSettings indexSettings, RepositoriesService repositoriesService) {
        super(indexSettings);
        Repository repo = repositoriesService.repository(REPOSITORY_NAME.get(indexSettings.getSettings()));
        if (repo instanceof BlobStoreRepository == false) {
            throw new UnsupportedOperationException("repository " + repo + " can't be used as a store repository");
        }
        repository = (BlobStoreRepository) repo;
        snapshotId = new SnapshotId(indexSettings.getValue(SNAPSHOT_NAME), indexSettings.getValue(SNAPSHOT_UUID));

    }

    @Override
    public DirectoryService newDirectoryService(ShardPath path) {
        return new DirectoryService(path.getShardId(), indexSettings) {
            @Override
            public Directory newDirectory() {
                IndexId indexId = repository.getRepositoryData().resolveIndexId(indexSettings.getIndex().getName());
                return repository.openDirectory(shardId, indexId, snapshotId);
            }
        };
    }
}
