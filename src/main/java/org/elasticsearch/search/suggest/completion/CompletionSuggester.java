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
package org.elasticsearch.search.suggest.completion;

import com.google.common.collect.Maps;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class CompletionSuggester implements Suggester<CompletionSuggestionContext> {

    private static final ScoreComparator scoreComparator = new ScoreComparator();

    @Override
    public Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> execute(String name,
            CompletionSuggestionContext suggestionContext, IndexReader indexReader, CharsRef spare) throws IOException {
        CompletionSuggestion completionSuggestionSuggestion = new CompletionSuggestion(name, suggestionContext.getSize());
        CompletionSuggestion.Entry completionSuggestEntry = new CompletionSuggestion.Entry(new StringText(suggestionContext.getText()
                .utf8ToString()), 0, suggestionContext.getText().toString().length());
        completionSuggestionSuggestion.addTerm(completionSuggestEntry);
        String fieldName = suggestionContext.getField();

        if (suggestionContext.mapper() == null || !(suggestionContext.mapper() instanceof CompletionFieldMapper)) {
            throw new ElasticSearchException("Field [" + suggestionContext.getField() + "] is not a completion suggest field");
        }
        String prefix = suggestionContext.getText().utf8ToString();

        Map<String, CompletionSuggestion.Entry.Option> results = Maps.newHashMapWithExpectedSize(indexReader.leaves().size() * suggestionContext.getSize());
        for (AtomicReaderContext atomicReaderContext : indexReader.leaves()) {
            AtomicReader atomicReader = atomicReaderContext.reader();
            Terms terms = atomicReader.fields().terms(fieldName);
            if (terms instanceof Completion090PostingsFormat.CompletionTerms) {
                Completion090PostingsFormat.CompletionTerms lookupTerms = (Completion090PostingsFormat.CompletionTerms) terms;
                Lookup lookup = lookupTerms.getLookup(suggestionContext.mapper(), false);
                List<Lookup.LookupResult> lookupResults = lookup.lookup(prefix, false, suggestionContext.getSize());
                for (Lookup.LookupResult res : lookupResults) {
                    
                    final String key = res.key.toString();
                    final float score = res.value;
                    final Option value = results.get(key);
                    if (value == null) {
                        final Option option = new CompletionSuggestion.Entry.Option(new StringText(key), score, res.payload == null ? null
                                : new BytesArray(res.payload));
                        results.put(key, option);
                    } else if (value.getScore() < score) {
                            value.setScore(score);
                            value.setPayload(res.payload == null ? null : new BytesArray(res.payload));
                    }
                }
            }
        }
        final List<CompletionSuggestion.Entry.Option> options = new ArrayList<CompletionSuggestion.Entry.Option>(results.values());
        CollectionUtil.introSort(options, scoreComparator);

        for (int i = 0 ; i < Math.min(suggestionContext.getSize(), options.size()) ; i++) {
            completionSuggestEntry.addOption(options.get(i));
        }

        return completionSuggestionSuggestion;
    }

    @Override
    public String[] names() {
        return new String[] { "completion" };
    }

    @Override
    public SuggestContextParser getContextParser() {
        return new CompletionSuggestParser(this);
    }

    public static class ScoreComparator implements Comparator<CompletionSuggestion.Entry.Option> {
        @Override
        public int compare(Option o1, Option o2) {
            return Float.compare(o2.getScore(), o1.getScore());
        }
    }
}
