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

package org.elasticsearch.test.integration.search.suggest;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.postingsformat.ElasticSearch090PostingsFormat;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PreBuiltPostingsFormatProvider;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.index.mapper.core.SuggestFieldMapper;
import org.elasticsearch.search.suggest.nrt.AnalyzingSuggestLookupProvider;
import org.elasticsearch.search.suggest.nrt.SuggestPostingsFormat.LookupFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Test
public class SuggestPostingsFormatTest {

    @Test
    public void testBasics() throws IOException {
        AnalyzingSuggestLookupProvider provider = new AnalyzingSuggestLookupProvider(true, 256, -1, true);
        RAMDirectory dir = new RAMDirectory();
        IndexOutput output = dir.createOutput("foo.txt", IOContext.DEFAULT);
        FieldsConsumer consumer = provider.consumer(output);
        FieldInfo fieldInfo = new FieldInfo("foo", true, 1, false, true, true, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, DocValuesType.SORTED, DocValuesType.BINARY, new HashMap<String,String>());
        TermsConsumer addField = consumer.addField(fieldInfo);
        
        PostingsConsumer postingsConsumer = addField.startTerm(new BytesRef("foofightersgenerator"));
        postingsConsumer.startDoc(0, 1);
        postingsConsumer.addPosition(256-2, provider.buildPayload(new BytesRef("Generator - Foo Fighters"), 9, new BytesRef("id:10")), 0, 1);
        postingsConsumer.finishDoc();
        addField.finishTerm(new BytesRef("foofightersgenerator"), new TermStats(1, 1));
        addField.startTerm(new BytesRef("generator"));
        postingsConsumer.startDoc(0, 1);
        postingsConsumer.addPosition(256-1, provider.buildPayload(new BytesRef("Generator - Foo Fighters"), 9, new BytesRef("id:10")), 0, 1);
        postingsConsumer.finishDoc();
        addField.finishTerm(new BytesRef("generator"), new TermStats(1, 1));
        addField.finish(1, 1, 1);
        consumer.close();
        output.close();
        
        IndexInput input = dir.openInput("foo.txt", IOContext.DEFAULT);
        LookupFactory load = provider.load(input);
        PostingsFormatProvider format = new PreBuiltPostingsFormatProvider(new ElasticSearch090PostingsFormat());
        NamedAnalyzer analyzer = new NamedAnalyzer("foo", new StandardAnalyzer(Version.LUCENE_43));
        Lookup lookup = load.getLookup(new SuggestFieldMapper(new Names("foo"), analyzer, analyzer, format, null, true, true), false);
        List<LookupResult> result = lookup.lookup("ge", false, 10);
        assertThat(result.get(0).key.toString(), equalTo("Generator - Foo Fighters"));
        assertThat(result.get(0).payload.utf8ToString(), equalTo("id:10"));
        dir.close();
    }
    
    // TODO ADD more unittests 
}
