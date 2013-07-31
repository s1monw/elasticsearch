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
package org.elasticsearch.test.unit.index.mapper.completion;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.test.unit.index.mapper.MapperTestUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CompletionFieldMapperTests {

    @Test
    public void testDefaultConfiguration() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        FieldMapper fieldMapper = defaultMapper.mappers().name("completion").mapper();
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper.class));

        CompletionFieldMapper completionFieldMapper = (CompletionFieldMapper) fieldMapper;
        assertThat(completionFieldMapper.isStoringPayloads(), is(false));
    }

    @Test
    public void testThatSerializationIncludesAllElements() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .field("index_analyzer", "simple")
                .field("search_analyzer", "standard")
                .field("payloads", true)
                .field("preserve_separators", false)
                .field("preserve_position_increments", true)
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        FieldMapper fieldMapper = defaultMapper.mappers().name("completion").mapper();
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper.class));

        CompletionFieldMapper completionFieldMapper = (CompletionFieldMapper) fieldMapper;
        XContentBuilder builder = jsonBuilder().startObject();
        completionFieldMapper.toXContent(builder, null).endObject();
        builder.close();
        Map<String, Object> serializedMap = JsonXContent.jsonXContent.createParser(builder.bytes()).mapAndClose();
        Map<String, Object> configMap = (Map<String, Object>) serializedMap.get("completion");
        assertThat(configMap.get("index_analyzer").toString(), is("simple"));
        assertThat(configMap.get("search_analyzer").toString(), is("standard"));
        assertThat(Boolean.valueOf(configMap.get("payloads").toString()), is(true));
        assertThat(Boolean.valueOf(configMap.get("preserve_separators").toString()), is(false));
        assertThat(Boolean.valueOf(configMap.get("preserve_position_increments").toString()), is(true));
    }

}
