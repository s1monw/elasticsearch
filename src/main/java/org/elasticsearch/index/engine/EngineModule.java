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

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Modules;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.internal.AppendEngineModule;
import org.elasticsearch.index.engine.internal.AppendIndexEngineModule;

/**
 *
 */
public class EngineModule extends AbstractModule implements SpawnModules {

    private final Settings settings;

    public EngineModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        Class<? extends Module> engineModule = IndexEngineModule.EngineSettings.DEFAULT_ENGINE;
        String engineType = settings.get(IndexEngineModule.EngineSettings.ENGINE_TYPE);
        if ("default".equals(engineType) || "versioned".equals(engineType)) {
            engineModule =  IndexEngineModule.EngineSettings.DEFAULT_ENGINE;
        } else if ("append".equals(engineType)) {
            engineModule = AppendEngineModule.class;
        } else {
            engineModule = settings.getAsClass(IndexEngineModule.EngineSettings.ENGINE_TYPE, engineModule, "org.elasticsearch.index.engine.", "EngineModule");
        }
        return ImmutableList.of(Modules.createModule(engineModule, settings));
    }

    @Override
    protected void configure() {
    }
}
