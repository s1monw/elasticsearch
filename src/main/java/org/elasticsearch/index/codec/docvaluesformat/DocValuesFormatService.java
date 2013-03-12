/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.codec.docvaluesformat;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Map;

/**
 * The {@link DocValuesFormatService} provides access to
 * all configured {@link DocValuesFormatProvider} instances by
 * {@link DocValuesFormatProvider#name() name}. 
 * 
 * @see CodecService 
 * 
 */
public class DocValuesFormatService extends AbstractIndexComponent {

    private final ImmutableMap<String, DocValuesFormatProvider> providers;

    public DocValuesFormatService(Index index) {
        this(index, ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public DocValuesFormatService(Index index, @IndexSettings Settings indexSettings) {
        this(index, indexSettings, ImmutableMap.<String, DocValuesFormatProvider.Factory>of());
    }

    @Inject
    public DocValuesFormatService(Index index, @IndexSettings Settings indexSettings, Map<String, DocValuesFormatProvider.Factory> postingFormatFactories) {
        super(index, indexSettings);

        MapBuilder<String, DocValuesFormatProvider> providers = MapBuilder.newMapBuilder();

        Map<String, Settings> postingsFormatSettings = indexSettings.getGroups(DocValuesFormatProvider.DOCVALUES_FORMAT_SETTINGS_PREFIX);
        for (Map.Entry<String, DocValuesFormatProvider.Factory> entry : postingFormatFactories.entrySet()) {
            String name = entry.getKey();
            DocValuesFormatProvider.Factory factory = entry.getValue();

            Settings settings = postingsFormatSettings.get(name);
            if (settings == null) {
                settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
            }
            providers.put(name, factory.create(name, settings));
        }

        // even though we have this logic in the cache module (where it should be, so posting format with delegates will work properly wiht the pre initialized map)
        // we do it here as well so we can use just this instance for tests
        for (PreBuiltDocValuesFormatProvider.Factory factory : DocValuesFormats.listFactories()) {
            if (providers.containsKey(factory.name())) {
                continue;
            }
            providers.put(factory.name(), factory.get());
        }

        this.providers = providers.immutableMap();
    }

    public DocValuesFormatProvider get(String name) throws ElasticSearchIllegalArgumentException {
        DocValuesFormatProvider provider = providers.get(name);
        if (name == null) {
            throw new ElasticSearchIllegalArgumentException("failed to find postings_format [" + name + "]");
        }
        return provider;
    }
}
