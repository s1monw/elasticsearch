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

package org.elasticsearch.search.suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.spell.SuggestWordFrequencyComparator;
import org.apache.lucene.search.spell.SuggestWordQueue;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastCharArrayReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.suggest.FuzzySuggest.FuzzySuggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;

import com.google.common.collect.ImmutableMap;

/**
 */
public class SuggestPhase extends AbstractComponent implements SearchPhase {

    @Inject
    public SuggestPhase(Settings settings) {
        super(settings);
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("suggest", new SuggestParseElement());
        return parseElements.build();
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    @Override
    public void execute(SearchContext context) throws ElasticSearchException {
        SuggestionSearchContext suggest = context.suggest();
        if (suggest == null) {
            return;
        }

        try {
            CharsRef spare = new CharsRef(); // Maybe add CharsRef to CacheRecycler?
            final Map<String, Suggestion<? extends Entry<? extends Option>>> suggestions = new HashMap<String,  Suggestion<? extends Entry<? extends Option>>>(2);
            for (Map.Entry<String, SuggestionSearchContext.Suggestion> entry : suggest.suggestions().entrySet()) {
                SuggestionSearchContext.Suggestion suggestion = entry.getValue();
                suggestions.put(entry.getKey(), executeDirectSpellChecker(entry.getKey(), suggestion, context, spare));
            }
            context.queryResult().suggest(new Suggest(suggestions));
        } catch (IOException e) {
            throw new ElasticSearchException("I/O exception during suggest phase", e);
        }
    }

    private FuzzySuggestion executeDirectSpellChecker(String name, SuggestionSearchContext.Suggestion suggestion, SearchContext context, CharsRef spare) throws IOException {
        DirectSpellChecker directSpellChecker = new DirectSpellChecker();
        directSpellChecker.setAccuracy(suggestion.accuracy());
        Comparator<SuggestWord> comparator;
        switch (suggestion.sort()) {
            case SCORE:
                comparator = SuggestWordQueue.DEFAULT_COMPARATOR;
                break;
            case FREQUENCY:
                comparator = LUCENE_FREQUENCY;
                break;
            default:
                throw new ElasticSearchIllegalArgumentException("Illegal suggest sort: " + suggestion.sort());
        }
        directSpellChecker.setComparator(comparator);
        directSpellChecker.setDistance(suggestion.stringDistance());
        directSpellChecker.setLowerCaseTerms(suggestion.lowerCaseTerms());
        directSpellChecker.setMaxEdits(suggestion.maxEdits());
        directSpellChecker.setMaxInspections(suggestion.factor());
        directSpellChecker.setMaxQueryFrequency(suggestion.maxTermFreq());
        directSpellChecker.setMinPrefix(suggestion.prefixLength());
        directSpellChecker.setMinQueryLength(suggestion.minWordLength());
        directSpellChecker.setThresholdFrequency(suggestion.minDocFreq());

        FuzzySuggestion response = new FuzzySuggestion(
                name, suggestion.size(), suggestion.sort()
        );
        List<Token> tokens = queryTerms(suggestion, spare);
        for (Token token : tokens) {
            IndexReader indexReader = context.searcher().getIndexReader();
            // TODO: Extend DirectSpellChecker in 4.1, to get the raw suggested words as BytesRef
            SuggestWord[] suggestedWords = directSpellChecker.suggestSimilar(
                    token.term, suggestion.shardSize(), indexReader, suggestion.suggestMode()
            );
            Text key = new BytesText(new BytesArray(token.term.bytes()));
            FuzzySuggestion.FuzzyEntry resultEntry = new FuzzySuggestion.FuzzyEntry(key, token.startOffset, token.endOffset - token.startOffset);
            for (SuggestWord suggestWord : suggestedWords) {
                Text word = new StringText(suggestWord.string);
                resultEntry.addOption(new FuzzySuggestion.FuzzyEntry.FuzzyOption(word, suggestWord.freq, suggestWord.score));
            }
            response.addTerm(resultEntry);
        }
        return response;
    }

    private List<Token> queryTerms(SuggestionSearchContext.Suggestion suggestion, CharsRef spare) throws IOException {
        UnicodeUtil.UTF8toUTF16(suggestion.text(), spare);
        TokenStream ts = suggestion.analyzer().tokenStream(
                suggestion.field(), new FastCharArrayReader(spare.chars, spare.offset, spare.length)
        );
        ts.reset();

        TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
        OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
        BytesRef termRef = termAtt.getBytesRef();

        List<Token> result = new ArrayList<Token>(5);
        while (ts.incrementToken()) {
            termAtt.fillBytesRef();
            Term term = new Term(suggestion.field(), BytesRef.deepCopyOf(termRef));
            result.add(new Token(term, offsetAtt.startOffset(), offsetAtt.endOffset()));
        }
        return result;
    }

    private static Comparator<SuggestWord> LUCENE_FREQUENCY = new SuggestWordFrequencyComparator();
   
    private static class Token {

        public final Term term;
        public final int startOffset;
        public final int endOffset;

        private Token(Term term, int startOffset, int endOffset) {
            this.term = term;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

    }

}
