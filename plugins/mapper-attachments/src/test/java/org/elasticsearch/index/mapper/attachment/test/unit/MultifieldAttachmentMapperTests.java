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

import java.nio.charset.StandardCharsets;

import org.elasticsearch.common.Base64;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.attachment.AttachmentMapper;
import org.elasticsearch.index.mapper.attachment.test.MapperTestUtils;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class MultifieldAttachmentMapperTests extends AttachmentUnitTestCase {

    private DocumentMapperParser mapperParser;
    private ThreadPool threadPool;

    @Before
    public void setupMapperParser() {
        mapperParser = MapperTestUtils.newMapperParser(createTempDir());
        mapperParser.putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());

    }

    @After
    public void cleanup() throws InterruptedException {
        terminate(threadPool);
    }

    @Test
    public void testSimpleMappings() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/multifield/multifield-mapping.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);


        assertThat(docMapper.mappers().getMapper("file.content"), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.content.suggest"), instanceOf(StringFieldMapper.class));

        assertThat(docMapper.mappers().getMapper("file.date"), instanceOf(DateFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.date.string"), instanceOf(StringFieldMapper.class));

        assertThat(docMapper.mappers().getMapper("file.title"), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.title.suggest"), instanceOf(StringFieldMapper.class));

        assertThat(docMapper.mappers().getMapper("file.name"), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.name.suggest"), instanceOf(StringFieldMapper.class));

        assertThat(docMapper.mappers().getMapper("file.author"), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.author.suggest"), instanceOf(StringFieldMapper.class));

        assertThat(docMapper.mappers().getMapper("file.keywords"), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.keywords.suggest"), instanceOf(StringFieldMapper.class));

        assertThat(docMapper.mappers().getMapper("file.content_type"), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.content_type.suggest"), instanceOf(StringFieldMapper.class));
    }

    @Test
    public void testExternalValues() throws Exception {
        String originalText = "This is an elasticsearch mapper attachment test.";
        String contentType = "text/plain; charset=ISO-8859-1";
        String forcedName = "dummyname.txt";

        String bytes = Base64.encodeBytes(originalText.getBytes(StandardCharsets.ISO_8859_1));
        threadPool = new ThreadPool("testing-only");

        MapperService mapperService = MapperTestUtils.newMapperService(createTempDir(), threadPool);
        mapperService.documentMapperParser().putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/multifield/multifield-mapping.json");

        DocumentMapper documentMapper = mapperService.documentMapperParser().parse(mapping);

        ParsedDocument doc = documentMapper.parse("person", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .field("file", bytes)
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("file.content"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content").stringValue(), is(originalText + "\n"));

        assertThat(doc.rootDoc().getField("file.content_type"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content_type").stringValue(), is(contentType));
        assertThat(doc.rootDoc().getField("file.content_type.suggest"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content_type.suggest").stringValue(), is(contentType));
        assertThat(doc.rootDoc().getField("file.content_length"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content_length").numericValue().intValue(), is(originalText.length()));

        assertThat(doc.rootDoc().getField("file.content.suggest"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content.suggest").stringValue(), is(originalText + "\n"));

        // Let's force some values
        doc = documentMapper.parse("person", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("file")
                        .field("_content", bytes)
                        .field("_name", forcedName)
                    .endObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("file.content"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content").stringValue(), is(originalText + "\n"));

        assertThat(doc.rootDoc().getField("file.content_type"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content_type").stringValue(), is(contentType));
        assertThat(doc.rootDoc().getField("file.content_type.suggest"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content_type.suggest").stringValue(), is(contentType));
        assertThat(doc.rootDoc().getField("file.content_length"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content_length").numericValue().intValue(), is(originalText.length()));

        assertThat(doc.rootDoc().getField("file.content.suggest"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content.suggest").stringValue(), is(originalText + "\n"));

        assertThat(doc.rootDoc().getField("file.name"), notNullValue());
        assertThat(doc.rootDoc().getField("file.name").stringValue(), is(forcedName));
        // In mapping we have default store:false
        assertThat(doc.rootDoc().getField("file.name").fieldType().stored(), is(false));
        assertThat(doc.rootDoc().getField("file.name.suggest"), notNullValue());
        assertThat(doc.rootDoc().getField("file.name.suggest").stringValue(), is(forcedName));
        // In mapping we set store:true for suggest subfield
        assertThat(doc.rootDoc().getField("file.name.suggest").fieldType().stored(), is(true));
    }
}
