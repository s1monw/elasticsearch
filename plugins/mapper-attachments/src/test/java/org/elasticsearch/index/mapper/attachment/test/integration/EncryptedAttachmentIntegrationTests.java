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

package org.elasticsearch.index.mapper.attachment.test.integration;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.plugins.PluginsService;
import org.junit.Test;
import org.apache.lucene.util.LuceneTestCase.Slow;

import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test case for issue https://github.com/elasticsearch/elasticsearch-mapper-attachments/issues/18
 */
@Slow
public class EncryptedAttachmentIntegrationTests extends AttachmentIntegrationTestCase {
    private boolean ignore_errors = true;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .build();
    }

    @Override
    public Settings indexSettings() {
        return settingsBuilder()
                .put("index.numberOfReplicas", 0)
                .put("index.mapping.attachment.ignore_errors", ignore_errors)
            .build();
    }

    /**
     * When we want to ignore errors (default)
     */
    @Test
    public void testMultipleAttachmentsWithEncryptedDoc() throws Exception {
        ignore_errors = true;
        logger.info("creating index [test]");
        createIndex("test");

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/integration/encrypted/test-mapping.json");
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/htmlWithValidDateMeta.html");
        byte[] pdf = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/encrypted.pdf");

        client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        index("test", "person", jsonBuilder().startObject().field("file1", html).field("file2", pdf).field("hello","world").endObject());
        refresh();


        CountResponse countResponse = client().prepareCount("test").setQuery(queryStringQuery("World").defaultField("file1.content")).execute().get();
        assertThatWithError(countResponse.getCount(), equalTo(1l));

        countResponse = client().prepareCount("test").setQuery(queryStringQuery("World").defaultField("hello")).execute().get();
        assertThat(countResponse.getCount(), equalTo(1l));
    }

    /**
     * When we don't want to ignore errors
     */
    @Test(expected = MapperParsingException.class)
    public void testMultipleAttachmentsWithEncryptedDocNotIgnoringErrors() throws Exception {
        ignore_errors = false;

        logger.info("creating index [test]");
        createIndex("test");

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/integration/encrypted/test-mapping.json");
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/htmlWithValidDateMeta.html");
        byte[] pdf = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/encrypted.pdf");

        client().admin().indices()
                .putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        index("test", "person", jsonBuilder().startObject().field("file1", html).field("file2", pdf).field("hello","world").endObject());
    }
}
