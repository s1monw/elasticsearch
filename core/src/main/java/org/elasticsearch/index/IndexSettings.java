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
package org.elasticsearch.index;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.PrimaryShardAllocator;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.indexing.IndexingSlowLog;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.search.stats.SearchSlowLog;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.MergePolicyConfig;
import org.elasticsearch.index.shard.MergeSchedulerConfig;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.indices.cache.request.IndicesRequestCache;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.search.internal.DefaultSearchContext;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * This class encapsulates all index level settings and handles settings updates.
 * It's created per index and available to all index level classes and allows them to retrieve
 * the latest updated settings instance. Classes that need to listen to settings updates can register
 * a settings consumer at index creation via {@link IndexModule#addIndexSettingsListener(Consumer)} that will
 * be called for each settings update.
 */
public final class IndexSettings extends AbstractScopedSettings {

    public static final String DEFAULT_FIELD = "index.query.default_field";
    public static final String QUERY_STRING_LENIENT = "index.query_string.lenient";
    public static final String QUERY_STRING_ANALYZE_WILDCARD = "indices.query.query_string.analyze_wildcard";
    public static final String QUERY_STRING_ALLOW_LEADING_WILDCARD = "indices.query.query_string.allowLeadingWildcard";
    public static final String ALLOW_UNMAPPED = "index.query.parse.allow_unmapped_fields";
    private final String uuid;
    private final Index index;
    private final Version version;
    private final String nodeName;
    private final Settings nodeSettings;
    private final int numberOfShards;
    private final boolean isShadowReplicaIndex;
    private final ParseFieldMatcher parseFieldMatcher;
    // volatile fields are updated via #updateIndexMetaData(IndexMetaData) under lock
    private volatile Settings settings;
    private volatile IndexMetaData indexMetaData;
    private final String defaultField;
    private final boolean queryStringLenient;
    private final boolean queryStringAnalyzeWildcard;
    private final boolean queryStringAllowLeadingWildcard;
    private final boolean defaultAllowUnmappedFields;
    private final Predicate<String> indexNameMatcher;

    /**
     * Returns the default search field for this index.
     */
    public String getDefaultField() {
        return defaultField;
    }

    /**
     * Returns <code>true</code> if query string parsing should be lenient. The default is <code>false</code>
     */
    public boolean isQueryStringLenient() {
        return queryStringLenient;
    }

    /**
     * Returns <code>true</code> if the query string should analyze wildcards. The default is <code>false</code>
     */
    public boolean isQueryStringAnalyzeWildcard() {
        return queryStringAnalyzeWildcard;
    }

    /**
     * Returns <code>true</code> if the query string parser should allow leading wildcards. The default is <code>true</code>
     */
    public boolean isQueryStringAllowLeadingWildcard() {
        return queryStringAllowLeadingWildcard;
    }

    /**
     * Returns <code>true</code> if queries should be lenient about unmapped fields. The default is <code>true</code>
     */
    public boolean isDefaultAllowUnmappedFields() {
        return defaultAllowUnmappedFields;
    }

    /**
     * Creates a new {@link IndexSettings} instance. The given node settings will be merged with the settings in the metadata
     * while index level settings will overwrite node settings.
     *
     * @param indexMetaData the index metadata this settings object is associated with
     * @param nodeSettings the nodes settings this index is allocated on.
     * @param registeredSettings a collection of listeners / consumers that should be notified if one or more settings are updated
     */
    public IndexSettings(final IndexMetaData indexMetaData, final Settings nodeSettings, final Set<Setting<?>> registeredSettings) {
        this(indexMetaData, nodeSettings, registeredSettings, (index) -> Regex.simpleMatch(index, indexMetaData.getIndex()));
    }

    /**
     * Creates a new {@link IndexSettings} instance. The given node settings will be merged with the settings in the metadata
     * while index level settings will overwrite node settings.
     *
     * @param indexMetaData the index metadata this settings object is associated with
     * @param nodeSettings the nodes settings this index is allocated on.
     * @param registeredSettings a collection of listeners / consumers that should be notified if one or more settings are updated
     * @param indexNameMatcher a matcher that can resolve an expression to the index name or index alias
     */
    public IndexSettings(final IndexMetaData indexMetaData, final Settings nodeSettings, Set<Setting<?>> registeredSettings, final Predicate<String> indexNameMatcher) {
        super(Settings.builder().put(nodeSettings).put(indexMetaData.getSettings()).build(), registeredSettings, Setting.Scope.INDEX);
        this.nodeSettings = nodeSettings;
        this.settings = Settings.builder().put(nodeSettings).put(indexMetaData.getSettings()).build();
        this.index = new Index(indexMetaData.getIndex());
        version = Version.indexCreated(settings);
        uuid = settings.get(IndexMetaData.SETTING_INDEX_UUID, IndexMetaData.INDEX_UUID_NA_VALUE);
        nodeName = settings.get("name", "");
        this.indexMetaData = indexMetaData;
        numberOfShards = settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, null);
        isShadowReplicaIndex = IndexMetaData.isIndexUsingShadowReplicas(settings);

        this.defaultField = settings.get(DEFAULT_FIELD, AllFieldMapper.NAME);
        this.queryStringLenient = settings.getAsBoolean(QUERY_STRING_LENIENT, false);
        this.queryStringAnalyzeWildcard = settings.getAsBoolean(QUERY_STRING_ANALYZE_WILDCARD, false);
        this.queryStringAllowLeadingWildcard = settings.getAsBoolean(QUERY_STRING_ALLOW_LEADING_WILDCARD, true);
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        this.defaultAllowUnmappedFields = settings.getAsBoolean(ALLOW_UNMAPPED, true);
        this.indexNameMatcher = indexNameMatcher;
        assert indexNameMatcher.test(indexMetaData.getIndex());
    }

    /**
     * Returns the settings for this index. These settings contain the node and index level settings where
     * settings that are specified on both index and node level are overwritten by the index settings.
     */
    public Settings getSettings() { return settings; }

    /**
     * Returns the index this settings object belongs to
     */
    public Index getIndex() {
        return index;
    }

    /**
     * Returns the indexes UUID
     */
    public String getUUID() {
        return uuid;
    }

    /**
     * Returns <code>true</code> if the index has a custom data path
     */
    public boolean hasCustomDataPath() {
        return customDataPath() != null;
    }

    /**
     * Returns the customDataPath for this index, if configured. <code>null</code> o.w.
     */
    public String customDataPath() {
        return settings.get(IndexMetaData.SETTING_DATA_PATH);
    }

    /**
     * Returns <code>true</code> iff the given settings indicate that the index
     * associated with these settings allocates it's shards on a shared
     * filesystem.
     */
    public boolean isOnSharedFilesystem() {
        return IndexMetaData.isOnSharedFilesystem(getSettings());
    }

    /**
     * Returns <code>true</code> iff the given settings indicate that the index associated
     * with these settings uses shadow replicas. Otherwise <code>false</code>. The default
     * setting for this is <code>false</code>.
     */
    public boolean isIndexUsingShadowReplicas() {
        return IndexMetaData.isOnSharedFilesystem(getSettings());
    }

    /**
     * Returns the version the index was created on.
     * @see Version#indexCreated(Settings)
     */
    public Version getIndexVersionCreated() {
        return version;
    }

    /**
     * Returns the current node name
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Returns the current IndexMetaData for this index
     */
    public IndexMetaData getIndexMetaData() {
        return indexMetaData;
    }

    /**
     * Returns the number of shards this index has.
     */
    public int getNumberOfShards() { return numberOfShards; }

    /**
     * Returns the number of replicas this index has.
     */
    public int getNumberOfReplicas() { return settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, null); }

    /**
     * Returns <code>true</code> iff this index uses shadow replicas.
     * @see IndexMetaData#isIndexUsingShadowReplicas(Settings)
     */
    public boolean isShadowReplicaIndex() { return isShadowReplicaIndex; }

    /**
     * Returns the node settings. The settings retured from {@link #getSettings()} are a merged version of the
     * index settings and the node settings where node settings are overwritten by index settings.
     */
    public Settings getNodeSettings() {
        return nodeSettings;
    }

    /**
     * Returns a {@link ParseFieldMatcher} for this index.
     */
    public ParseFieldMatcher getParseFieldMatcher() { return parseFieldMatcher; }

    /**
     * Returns <code>true</code> if the given expression matches the index name or one of it's aliases
     */
    public boolean matchesIndexName(String expression) {
        return indexNameMatcher.test(expression);
    }

    /**
     * Updates the settings and index metadata and notifies all registered settings consumers with the new settings iff at least one setting has changed.
     *
     * @return <code>true</code> iff any setting has been updated otherwise <code>false</code>.
     */
    synchronized boolean updateIndexMetaData(IndexMetaData indexMetaData) {
        final Settings newSettings = indexMetaData.getSettings();
        if (Version.indexCreated(newSettings) != version) {
            throw new IllegalArgumentException("version mismatch on settings update expected: " + version + " but was: " + Version.indexCreated(newSettings));
        }
        final String newUUID = newSettings.get(IndexMetaData.SETTING_INDEX_UUID, IndexMetaData.INDEX_UUID_NA_VALUE);
        if (newUUID.equals(getUUID()) == false) {
            throw new IllegalArgumentException("uuid mismatch on settings update expected: " + uuid + " but was: " + newUUID);
        }
        this.indexMetaData = indexMetaData;
        final Settings existingSettings = this.settings;
        if (existingSettings.getByPrefix(IndexMetaData.INDEX_SETTING_PREFIX).getAsMap().equals(newSettings.getByPrefix(IndexMetaData.INDEX_SETTING_PREFIX).getAsMap())) {
            // nothing to update, same settings
            return false;
        }

        this.settings = applySettings(newSettings);
        return true;
    }

    public static Set<Setting<?>> BUILT_IN_CLUSTER_SETTINGS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        IndexStore.INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING, /* Validator.BYTES_SIZE */
    IndexStore.INDEX_STORE_THROTTLE_TYPE_SETTING, /* Validator.EMPTY */
    MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING, /* Validator.NON_NEGATIVE_INTEGER */
    MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING, /* Validator.EMPTY */
    MergeSchedulerConfig.AUTO_THROTTLE_SETTING, /* Validator.EMPTY */
    FilterAllocationDecider.INDEX_ROUTING_REQUIRE_GROUP+ "*", /* Validator.EMPTY */
    FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "*", /* Validator.EMPTY */
    FilterAllocationDecider.INDEX_ROUTING_EXCLUDE_GROUP + "*", /* Validator.EMPTY */
    EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE, /* Validator.EMPTY */
    EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE, /* Validator.EMPTY */
    TranslogConfig.INDEX_TRANSLOG_FS_TYPE, /* Validator.EMPTY */
    IndexMetaData.SETTING_NUMBER_OF_REPLICAS, /* Validator.NON_NEGATIVE_INTEGER */
    IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, /* Validator.EMPTY */
    IndexMetaData.SETTING_READ_ONLY, /* Validator.EMPTY */
    IndexMetaData.SETTING_BLOCKS_READ, /* Validator.EMPTY */
    IndexMetaData.SETTING_BLOCKS_WRITE, /* Validator.EMPTY */
    IndexMetaData.SETTING_BLOCKS_METADATA, /* Validator.EMPTY */
    IndexMetaData.SETTING_SHARED_FS_ALLOW_RECOVERY_ON_ANY_NODE, /* Validator.EMPTY */
    IndexMetaData.SETTING_PRIORITY, /* Validator.NON_NEGATIVE_INTEGER */
    IndicesTTLService.INDEX_TTL_DISABLE_PURGE, /* Validator.EMPTY */
    IndexShard.INDEX_REFRESH_INTERVAL, /* Validator.TIME */
    PrimaryShardAllocator.INDEX_RECOVERY_INITIAL_SHARDS, /* Validator.EMPTY */
    EngineConfig.INDEX_COMPOUND_ON_FLUSH, /* Validator.BOOLEAN */
    EngineConfig.INDEX_GC_DELETES_SETTING, /* Validator.TIME */
    IndexShard.INDEX_FLUSH_ON_CLOSE, /* Validator.BOOLEAN */
    EngineConfig.INDEX_VERSION_MAP_SIZE, /* Validator.BYTES_SIZE_OR_PERCENTAGE */
    IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN, /* Validator.TIME */
    IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO, /* Validator.TIME */
    IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG, /* Validator.TIME */
    IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE, /* Validator.TIME */
    IndexingSlowLog.INDEX_INDEXING_SLOWLOG_REFORMAT, /* Validator.EMPTY */
    IndexingSlowLog.INDEX_INDEXING_SLOWLOG_LEVEL, /* Validator.EMPTY */
    IndexingSlowLog.INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG, /* Validator.EMPTY */
    SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN, /* Validator.TIME */
    SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO, /* Validator.TIME */
    SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG, /* Validator.TIME */
    SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE, /* Validator.TIME */
    SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN, /* Validator.TIME */
    SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO, /* Validator.TIME */
    SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG, /* Validator.TIME */
    SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE, /* Validator.TIME */
    SearchSlowLog.INDEX_SEARCH_SLOWLOG_REFORMAT, /* Validator.EMPTY */
    SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL, /* Validator.EMPTY */
    ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE, /* Validator.INTEGER */
    MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED, /* Validator.DOUBLE */
    MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT, /* Validator.BYTES_SIZE */
    MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE, /* Validator.INTEGER_GTE_2 */
    MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT, /* Validator.INTEGER_GTE_2 */
    MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT, /* Validator.BYTES_SIZE */
    MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER, /* Validator.DOUBLE_GTE_2 */
    MergePolicyConfig.INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT, /* Validator.NON_NEGATIVE_DOUBLE */
    MergePolicyConfig.INDEX_COMPOUND_FORMAT, /* Validator.EMPTY */
    IndexShard.INDEX_TRANSLOG_FLUSH_THRESHOLD_OPS, /* Validator.INTEGER */
    IndexShard.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE, /* Validator.BYTES_SIZE */
    IndexShard.INDEX_TRANSLOG_DISABLE_FLUSH, /* Validator.EMPTY */
    TranslogConfig.INDEX_TRANSLOG_DURABILITY, /* Validator.EMPTY */
    IndicesWarmer.INDEX_WARMER_ENABLED, /* Validator.EMPTY */
    IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED, /* Validator.BOOLEAN */
    IndicesRequestCache.DEPRECATED_INDEX_CACHE_REQUEST_ENABLED, /* Validator.BOOLEAN */
    UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING, /* Validator.TIME */
    DefaultSearchContext.MAX_RESULT_WINDOW /* Validator.POSITIVE_INTEGER */
    )));
}
