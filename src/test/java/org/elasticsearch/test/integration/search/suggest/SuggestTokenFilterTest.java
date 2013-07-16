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

import org.elasticsearch.search.suggest.nrt.SuggestTokenFilter;

import org.apache.lucene.analysis.core.SimpleAnalyzer;

import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;

import org.apache.lucene.util.IntsRef;

import org.apache.lucene.util.IntsRef;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SuggestTokenFilterTest {

    RAMDirectory ramDirectory = new RAMDirectory();
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());

    @Test
    public void testSuggestTokenFilter() throws Exception {
        IndexWriter indexWriter = new IndexWriter(ramDirectory, indexWriterConfig);
        final XAnalyzingSuggester suggester = new XAnalyzingSuggester(new SimpleAnalyzer(Version.LUCENE_43));
        TokenStream tokenStream = new KeywordAnalyzer().tokenStream("", new StringReader("meinKeyWord"));
        TokenStream suggestTokenStream = new SuggestTokenFilter(tokenStream, new BytesRef("Surface keyword|friggin payload|10"), new SuggestTokenFilter.ToFiniteStrings() {
            @Override
            public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException {
                return suggester.toFiniteStrings(suggester.getTokenStreamToAutomaton(), stream);
            }
        });
        Field suggestField = new FooField("testField", suggestTokenStream);
        Document doc = new Document();
        doc.add(suggestField);

        indexWriter.addDocument(doc);
        indexWriter.commit();

        IndexReader indexReader = DirectoryReader.open(ramDirectory);
        assertThat(indexReader.leaves().size(), is(1));
        AtomicReader atomicReader = indexReader.leaves().get(0).reader();
        Terms terms = atomicReader.fields().terms("testField");
        TermsEnum termsEnum = terms.iterator(null);
//        assertThat(termsEnum.term().utf8ToString(), is("meinKeyword"));

        BytesRef term = null;
        DocsAndPositionsEnum docsAndPositionsEnum = null;
        while ((term = termsEnum.next()) != null) {
            docsAndPositionsEnum = termsEnum.docsAndPositions(null, docsAndPositionsEnum);
            // TODO CALL ME ONCE AND IT WORKS?!
            docsAndPositionsEnum.nextPosition();

            assertThat(term.utf8ToString(), is("meinKeyWord"));
//            if (docsAndPositionsEnum.getPayload() != null) {
//                System.out.println(docsAndPositionsEnum.getPayload().utf8ToString());
//            }
        }


        BytesRef payload = docsAndPositionsEnum.getPayload();
        assertThat(payload, is(notNullValue()));
        assertThat(payload.utf8ToString(), is(notNullValue()));
        assertThat(payload.utf8ToString(), is("Surface keyword|friggin payload|10"));
    }


    public static class FooField extends Field {

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexed(true);
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
            FIELD_TYPE.freeze();
        }

        public FooField(String name, TokenStream stream) {
            super(name, stream, FIELD_TYPE);
        }
    }
}
