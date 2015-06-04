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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.attachment.AttachmentMapper;
import org.elasticsearch.index.mapper.attachment.test.MapperTestUtils;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 *
 */
public class LanguageDetectionAttachmentMapperTests extends AttachmentUnitTestCase {

    private DocumentMapper docMapper;

    @Before
    public void setupMapperParser() throws IOException {
        setupMapperParser(true);
    }

    public void setupMapperParser(boolean langDetect) throws IOException {
        DocumentMapperParser mapperParser = MapperTestUtils.newMapperParser(
                Settings.settingsBuilder()
                                  .put("path.home", createTempDir())
                                  .put("index.mapping.attachment.detect_language", langDetect)
                                 .build());
        mapperParser.putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/language/language-mapping.json");
        docMapper = mapperParser.parse(mapping);

        assertThat(docMapper.mappers().getMapper("file.language"), instanceOf(StringFieldMapper.class));
    }

    private void testLanguage(String filename, String expected, String... forcedLanguage) throws IOException {
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/" + filename);

        XContentBuilder xcb = jsonBuilder()
                .startObject()
                    .startObject("file")
                        .field("_name", filename)
                        .field("_content", html);

        if (forcedLanguage.length > 0) {
            xcb.field("_language", forcedLanguage[0]);
        }

        xcb.endObject().endObject();

        ParseContext.Document doc =  docMapper.parse("person", "1", xcb.bytes()).rootDoc();

        // Our mapping should be kept as a String
        assertThat(doc.get(docMapper.mappers().getMapper("file.language").names().indexName()), equalTo(expected));
    }

    @Test
    public void testFrDetection() throws Exception {
        testLanguage("text-in-french.txt", "fr");
    }

    @Test
    public void testEnDetection() throws Exception {
        testLanguage("text-in-english.txt", "en");
    }

    @Test
    public void testFrForced() throws Exception {
        testLanguage("text-in-english.txt", "fr", "fr");
    }

    /**
     * This test gives strange results! detection of ":-)" gives "lt" as a result
     * @throws Exception
     */
    @Test
    public void testNoLanguage() throws Exception {
        testLanguage("text-in-nolang.txt", "lt");
    }

    @Test
    public void testLangDetectDisabled() throws Exception {
        // We replace the mapper with another one which have index.mapping.attachment.detect_language = false
        setupMapperParser(false);
        testLanguage("text-in-english.txt", null);
    }

    @Test
    public void testLangDetectDocumentEnabled() throws Exception {
        // We replace the mapper with another one which have index.mapping.attachment.detect_language = false
        setupMapperParser(false);

        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/text-in-english.txt");

        XContentBuilder xcb = jsonBuilder()
                .startObject()
                .startObject("file")
                    .field("_name", "text-in-english.txt")
                    .field("_content", html)
                    .field("_detect_language", true)
                .endObject().endObject();

        ParseContext.Document doc =  docMapper.parse("person", "1", xcb.bytes()).rootDoc();

        // Our mapping should be kept as a String
        assertThat(doc.get(docMapper.mappers().getMapper("file.language").names().indexName()), equalTo("en"));
    }
}
