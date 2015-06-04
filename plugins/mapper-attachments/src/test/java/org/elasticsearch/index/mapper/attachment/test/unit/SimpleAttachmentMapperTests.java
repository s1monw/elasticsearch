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

package org.elasticsearch.index.mapper.attachment.test.unit;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.attachment.AttachmentMapper;
import org.elasticsearch.index.mapper.attachment.test.MapperTestUtils;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleAttachmentMapperTests extends AttachmentUnitTestCase {

    @Test
    public void testSimpleMappings() throws Exception {
        DocumentMapperParser mapperParser = MapperTestUtils.newMapperParser(createTempDir());
        mapperParser.putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/simple/test-mapping.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/testXHTML.html");

        BytesReference json = jsonBuilder().startObject().field("file", html).endObject().bytes();

        ParseContext.Document doc = docMapper.parse("person", "1", json).rootDoc();

        assertThat(doc.get(docMapper.mappers().getMapper("file.content_type").names().indexName()), startsWith("application/xhtml+xml"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.title").names().indexName()), equalTo("XHTML test document"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.content").names().indexName()), containsString("This document tests the ability of Apache Tika to extract content"));

        // re-parse it
        String builtMapping = docMapper.mappingSource().string();
        docMapper = mapperParser.parse(builtMapping);

        json = jsonBuilder().startObject().field("file", html).endObject().bytes();

        doc = docMapper.parse("person", "1", json).rootDoc();

        assertThat(doc.get(docMapper.mappers().getMapper("file.content_type").names().indexName()), startsWith("application/xhtml+xml"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.title").names().indexName()), equalTo("XHTML test document"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.content").names().indexName()), containsString("This document tests the ability of Apache Tika to extract content"));
    }

    public void testContentBackcompat() throws Exception {
        DocumentMapperParser mapperParser = MapperTestUtils.newMapperParser(Settings.builder()
            .put("path.home", createTempDir())
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id)
            .build());
        mapperParser.putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/simple/test-mapping.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/testXHTML.html");

        BytesReference json = jsonBuilder().startObject().field("file", html).endObject().bytes();

        ParseContext.Document doc = docMapper.parse("person", "1", json).rootDoc();
        assertThat(doc.get("file"), containsString("This document tests the ability of Apache Tika to extract content"));
    }
}
