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

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene42.Lucene42DocValuesFormat;
import org.elasticsearch.common.collect.MapBuilder;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;

public class DocValuesFormats {
    private static final ImmutableMap<String, PreBuiltDocValuesFormatProvider.Factory> builtInPostingFormats;

    static {
        MapBuilder<String, PreBuiltDocValuesFormatProvider.Factory> buildInPostingFormatsX = MapBuilder.newMapBuilder();
        buildInPostingFormatsX.put("default", new PreBuiltDocValuesFormatProvider.Factory("default", new Lucene42DocValuesFormat()));
        // add defaults ones
        for (String luceneName : DocValuesFormat.availableDocValuesFormats()) {
            buildInPostingFormatsX.put(luceneName, new PreBuiltDocValuesFormatProvider.Factory(DocValuesFormat.forName(luceneName)));
        }
        builtInPostingFormats = buildInPostingFormatsX.immutableMap();
    }

    public static DocValuesFormatProvider.Factory getAsFactory(String name) {
        return builtInPostingFormats.get(name);
    }

    public static DocValuesFormatProvider getAsProvider(String name) {
        return builtInPostingFormats.get(name).get();
    }

    public static ImmutableCollection<PreBuiltDocValuesFormatProvider.Factory> listFactories() {
        return builtInPostingFormats.values();
    }
}
