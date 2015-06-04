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

package org.elasticsearch.plugin.cloud.gce;

import org.elasticsearch.cloud.gce.GceModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 */
public class CloudGcePlugin extends AbstractPlugin {

    private final Settings settings;

    public CloudGcePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "cloud-gce";
    }

    @Override
    public String description() {
        return "Cloud Google Compute Engine Plugin";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = new ArrayList<>();
        if (settings.getAsBoolean("cloud.enabled", true)) {
            modules.add(GceModule.class);
        }
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        if (settings.getAsBoolean("cloud.enabled", true)) {
//            services.add(GceComputeServiceImpl.class);
        }
        return services;
    }

}
