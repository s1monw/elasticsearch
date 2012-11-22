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

package org.elasticsearch.test.unit.index.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.KeepWordFilterFactory;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

public class KeepFilterFactoryTests {

    private static final String RESOURCE = "org/elasticsearch/test/unit/index/analysis/keep_analysis.json";

    @Test
    public void testCaseInsensitiveMapping() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_keep_filter");
        assertThat(tokenFilter, instanceOf(KeepWordFilterFactory.class));
        String source = "hello small world";
        String[] expected = new String[]{"hello", "world"};
        Tokenizer tokenizer = new WhitespaceTokenizer(Version.LUCENE_40, new StringReader(source));
        AnalysisTestsHelper.assertSimpleTSOutput(tokenFilter.create(tokenizer), expected, new int[] {1,2});
    }

    @Test
    public void testCaseSensitiveMapping() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_case_sensitive_keep_filter");
        assertThat(tokenFilter, instanceOf(KeepWordFilterFactory.class));
        String source = "Hello small world";
        String[] expected = new String[]{"Hello"};
        Tokenizer tokenizer = new WhitespaceTokenizer(Version.LUCENE_40, new StringReader(source));
        AnalysisTestsHelper.assertSimpleTSOutput(tokenFilter.create(tokenizer), expected, new int[] {1});
    }

}
