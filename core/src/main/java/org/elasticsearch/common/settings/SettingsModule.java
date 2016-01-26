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

package org.elasticsearch.common.settings;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.inject.AbstractModule;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * A module that binds the provided settings to the {@link Settings} interface.
 *
 *
 */
public class SettingsModule extends AbstractModule {
    static BufferedWriter bufferedWriter;

    static {
       doIt();
    }

    @SuppressForbidden(reason = "I know what I am doing")
    private static void doIt() {
        Path path = Paths.get("/home/simon/settings");
        try {
            Files.createDirectory(path);
            Path file = Files.createTempFile(path, "settings", ".txt");
            bufferedWriter = Files.newBufferedWriter(file);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private final Settings settings;
    private final SettingsFilter settingsFilter;
    private final Map<String, Setting<?>> clusterSettings = new HashMap<>();
    private final Map<String, Setting<?>> indexSettings = new HashMap<>();

    public SettingsModule(Settings settings, SettingsFilter settingsFilter) {
        this.settings = settings;
        this.settingsFilter = settingsFilter;
        for (Setting<?> setting : ClusterSettings.BUILT_IN_CLUSTER_SETTINGS) {
            registerSetting(setting);
        }
        for (Setting<?> setting : IndexScopedSettings.BUILT_IN_INDEX_SETTINGS) {
            registerSetting(setting);
        }
    }

    @Override
    protected void configure() {
        final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(settings, new HashSet<>(this.indexSettings.values()));
        final ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(this.clusterSettings.values()));
        // by now we are fully configured, lets check node level settings for unregistered index settings
        indexScopedSettings.validate(settings.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE));
        // we can't call this method yet since we have not all node level settings registered.
        // yet we can validate the ones we have registered to not have invalid values. this is better than nothing
        // and progress over perfection and we fail as soon as possible.
        // clusterSettings.validate(settings.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE.negate()));
        for (Map.Entry<String, String> entry : settings.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE.negate()).getAsMap().entrySet()) {
            if (clusterSettings.get(entry.getKey()) != null) {
                clusterSettings.validate(entry.getKey(), settings);
            } else if (AbstractScopedSettings.isValidKey(entry.getKey()) == false) {
                throw new IllegalArgumentException("illegal settings key: [" + entry.getKey() + "]");
            } else {
                try {
                    bufferedWriter.write(entry.getKey());
                    bufferedWriter.write("\n");
                } catch (IOException boom) {
                    throw new RuntimeException("boom", boom);
                }
            }
        }

        bind(Settings.class).toInstance(settings);
        bind(SettingsFilter.class).toInstance(settingsFilter);

        bind(ClusterSettings.class).toInstance(clusterSettings);
        bind(IndexScopedSettings.class).toInstance(indexScopedSettings);
    }

    public void registerSetting(Setting<?> setting) {
        switch (setting.getScope()) {
            case CLUSTER:
                if (clusterSettings.containsKey(setting.getKey())) {
                    throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
                }
                clusterSettings.put(setting.getKey(), setting);
                break;
            case INDEX:
                if (indexSettings.containsKey(setting.getKey())) {
                    throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
                }
                indexSettings.put(setting.getKey(), setting);
                break;
        }
    }

}
