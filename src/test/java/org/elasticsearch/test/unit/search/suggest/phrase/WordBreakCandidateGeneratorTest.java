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
package org.elasticsearch.test.unit.search.suggest.phrase;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.search.suggest.phrase.CandidateSet;
import org.elasticsearch.search.suggest.phrase.WordBreakCandidateGenerator;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
public class WordBreakCandidateGeneratorTest {

    
    @Test
    public void testSimple() throws IOException {
        RAMDirectory dir = new RAMDirectory();
        Map<String, Analyzer> mapping = new HashMap<String, Analyzer>();
        mapping.put("body_ngram", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Tokenizer t = new StandardTokenizer(Version.LUCENE_41, reader);
                ShingleFilter tf = new ShingleFilter(t, 2, 3);
                tf.setOutputUnigrams(false);
                return new TokenStreamComponents(t, new LowerCaseFilter(Version.LUCENE_41, tf));
            }

        });

        mapping.put("body", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Tokenizer t = new StandardTokenizer(Version.LUCENE_41, reader);
                return new TokenStreamComponents(t, new LowerCaseFilter(Version.LUCENE_41, t));
            }

        });
        PerFieldAnalyzerWrapper wrapper = new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer(Version.LUCENE_41), mapping);

        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_41, wrapper);
        IndexWriter writer = new IndexWriter(dir, conf);
        String[] docs = new String[] { 
                "hello world",
                "jo pizza",
                "christianspor",
                "christian spor",
                "christian spor",
        };
        for (String string : docs) {
            Document doc = new Document();
            doc.add(new Field("body", string, TextField.TYPE_NOT_STORED));
            doc.add(new Field("body_ngram", string, TextField.TYPE_NOT_STORED));
            writer.addDocument(doc);    
        }
        
        DirectoryReader ir = DirectoryReader.open(writer, false);
        WordBreakCandidateGenerator generartor = new WordBreakCandidateGenerator(ir, "body_ngram", 3, 3);
        CandidateSet set = generartor.drawCandidates(new BytesRef("helloworld"));
        assertThat(set.candidates.length, equalTo(1));
        assertThat(set.candidates[0].term.utf8ToString(), equalTo("hello world"));
        assertThat(set.candidates[0].frequency, equalTo(1l));
        
        
        set = generartor.drawCandidates(new BytesRef("christianspor"));
        assertThat(set.candidates.length, equalTo(1));
        assertThat(set.candidates[0].term.utf8ToString(), equalTo("christian spor"));
        assertThat(set.candidates[0].frequency, equalTo(2l));
        
        
        set = generartor.drawCandidates(new BytesRef("jopizza"));
        assertThat(set.candidates.length, equalTo(0));
    }
        
}
