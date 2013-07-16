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
package org.elasticsearch.search.suggest.nrt;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.util.CharsRef;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.nrt.NrtSuggestion.Entry.Option;
import org.elasticsearch.search.suggest.nrt.SuggestPostingsFormat.LookupTerms;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * DONE: copy wfst completion lookup and make it cheap :)
 *
 */
public class NrtSuggester implements  Suggester<NrtSuggestionContext> {

    @Override
    public Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> execute(String name, NrtSuggestionContext suggestion, IndexReader indexReader, CharsRef spare) throws IOException {
        NrtSuggestion wfstSuggestion = new NrtSuggestion(name, suggestion.getSize());
        NrtSuggestion.Entry wfstSuggestionEntry = new NrtSuggestion.Entry(new StringText(suggestion.getText().utf8ToString()), 0, suggestion.getText().toString().length());
        wfstSuggestion.addTerm(wfstSuggestionEntry);
        String fieldName = suggestion.getField();

        String prefix = suggestion.getText().utf8ToString(); // NOCOMMIT - this needs to go through an analyzer?
        boolean exactFirst = true; // TODO Expose in request?
        // do the suggestion dance per segment
        Map<CharSequence, Long> allResults = new HashMap<CharSequence, Long>();
        for (AtomicReaderContext atomicReaderContext : indexReader.leaves()) {
            AtomicReader atomicReader = atomicReaderContext.reader();
            Terms terms = atomicReader.fields().terms(fieldName);
            if (terms instanceof SuggestPostingsFormat.LookupTerms) {
                LookupTerms lookupTerms = (LookupTerms) terms;
                Lookup lookup = lookupTerms.getLookup(suggestion.mapper(), exactFirst);
                List<Lookup.LookupResult> lookupResults = lookup.lookup(prefix, false, suggestion.getSize());
                for (Lookup.LookupResult res : lookupResults) {
                    Long weight = allResults.get(res);
                    if (weight == null) {
                        allResults.put(res.key, res.value);
                    } else {
                        allResults.put(res.key, Math.max(res.value, weight));
                    }
                }
                
            }
       }
        Set<Entry<CharSequence, Long>> entrySet = allResults.entrySet();
        List<NrtSuggestion.Entry.Option> options = new ArrayList<NrtSuggestion.Entry.Option>();
        for(Entry<CharSequence, Long> entry : entrySet) {
            options.add(new NrtSuggestion.Entry.Option(new StringText(entry.getKey().toString()), entry.getValue()));
        }
        Collections.sort(options, new Comparator<NrtSuggestion.Entry.Option>() {

            @Override
            public int compare(Option o1, Option o2) {
                return Float.compare(o2.getScore(), o1.getScore());
            }
        });
        int size = suggestion.getSize();
        for(NrtSuggestion.Entry.Option o : options) {
            if (size-- == 0) {
                break;
            }
            wfstSuggestionEntry.addOption(o);
        }

        return wfstSuggestion;
    }

    @Override
    public String[] names() {
        return new String[] { "nrt" };
    }

    @Override
    public SuggestContextParser getContextParser() {
        return new NrtSuggestParser(this);
    }
   

}
