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
package org.elasticsearch.search.suggest.phrase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.search.suggest.SuggestUtils;

//TODO public for tests
public final class DirectCandidateGenerator extends CandidateGenerator {

    private final DirectSpellChecker spellchecker;
    private final String field;
    private final SuggestMode suggestMode;
    private final TermsEnum termsEnum;
    private final IndexReader reader;
    private final long dictSize;
    private final double logBase = 5;
    private final long frequencyPlateau;
    private final Analyzer preFilter;
    private final Analyzer postFilter;
    private final double nonErrorLikelihood;
    private final boolean useTotalTermFrequency;
    private final CharsRef spare = new CharsRef();
    private final BytesRef byteSpare = new BytesRef();
    private final int numCandidates;
    
    public DirectCandidateGenerator(DirectSpellChecker spellchecker, String field, SuggestMode suggestMode, IndexReader reader, double nonErrorLikelihood, int numCandidates) throws IOException {
        this(spellchecker, field, suggestMode, reader,  nonErrorLikelihood, numCandidates, null, null);
    }


    public DirectCandidateGenerator(DirectSpellChecker spellchecker, String field, SuggestMode suggestMode, IndexReader reader, double nonErrorLikelihood,  int numCandidates, Analyzer preFilter, Analyzer postFilter) throws IOException {
        this.spellchecker = spellchecker;
        this.field = field;
        this.numCandidates = numCandidates;
        this.suggestMode = suggestMode;
        this.reader = reader;
        Terms terms = MultiFields.getTerms(reader, field);
        if (terms == null) {
            throw new ElasticSearchIllegalArgumentException("generator field [" + field + "] doesn't exist");
        }
        final long dictSize = terms.getSumTotalTermFreq();
        this.useTotalTermFrequency = dictSize != -1;
        this.dictSize =  dictSize == -1 ? reader.maxDoc() : dictSize;
        this.preFilter = preFilter;
        this.postFilter = postFilter;
        this.nonErrorLikelihood = nonErrorLikelihood;
        float thresholdFrequency = spellchecker.getThresholdFrequency();
        this.frequencyPlateau = thresholdFrequency >= 1.0f ? (int) thresholdFrequency: (int)(dictSize * thresholdFrequency);
        termsEnum = terms.iterator(null);
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.suggest.phrase.CandidateGenerator#frequency(org.apache.lucene.util.BytesRef)
     */
    @Override
    public long frequency(BytesRef term) throws IOException {
        term = preFilter(term, spare, byteSpare);
        return internalFrequency(term);
    }


    public long internalFrequency(BytesRef term) throws IOException {
        if (termsEnum.seekExact(term, true)) {
            return useTotalTermFrequency ? termsEnum.totalTermFreq() : termsEnum.docFreq(); 
        }
        return 0;
    }
    
    public String getField() {
        return field;
    }
    
    /* (non-Javadoc)
     * @see org.elasticsearch.search.suggest.phrase.CandidateGenerator#drawCandidates(org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.CandidateSet, int)
     */
    @Override
    public CandidateSet drawCandidates(CandidateSet set) throws IOException {
        Candidate original = set.originalTerm;
        BytesRef term = preFilter(original.term, spare, byteSpare);
        final long frequency = original.frequency;
        spellchecker.setThresholdFrequency(thresholdFrequency(frequency, dictSize));
        SuggestWord[] suggestSimilar = spellchecker.suggestSimilar(new Term(field, term), numCandidates, reader, this.suggestMode);
        List<Candidate> candidates = new ArrayList<Candidate>(suggestSimilar.length);
        for (int i = 0; i < suggestSimilar.length; i++) {
            SuggestWord suggestWord = suggestSimilar[i];
            BytesRef candidate = new BytesRef(suggestWord.string);
            postFilter(new Candidate(candidate, internalFrequency(candidate), suggestWord.score, score(suggestWord.freq, suggestWord.score)), spare, byteSpare, candidates);
        }
        set.addCandidates(candidates);
        return set;
    }
    
    protected BytesRef preFilter(final BytesRef term, final CharsRef spare, final BytesRef byteSpare) throws IOException {
        if (preFilter == null) {
            return term;
        }
        final BytesRef result = byteSpare;
        SuggestUtils.analyze(preFilter, term, field, new SuggestUtils.TokenConsumer() {
            
            @Override
            public void nextToken() throws IOException {
                this.fillBytesRef(result);
            }
        }, spare);
        return result;
    }
    
    protected void postFilter(final Candidate candidate, final CharsRef spare, BytesRef byteSpare, final List<Candidate> candidates) throws IOException {
        if (postFilter == null) {
            candidates.add(candidate);
        } else {
            final BytesRef result = byteSpare;
            SuggestUtils.analyze(postFilter, candidate.term, field, new SuggestUtils.TokenConsumer() {
                @Override
                public void nextToken() throws IOException {
                    this.fillBytesRef(result);
                    
                    if (posIncAttr.getPositionIncrement() > 0 && result.bytesEquals(candidate.term))  {
                        BytesRef term = BytesRef.deepCopyOf(result);    
                        long freq = frequency(term);
                        candidates.add(new Candidate(BytesRef.deepCopyOf(term), freq, candidate.stringDistance, score(candidate.frequency, candidate.stringDistance)));
                    } else {
                        candidates.add(new Candidate(BytesRef.deepCopyOf(result), candidate.frequency, nonErrorLikelihood, score(candidate.frequency, candidate.stringDistance)));
                    }
                }
            }, spare);
        }
    }
    
    @Override
    protected double score(long frequency, double errorScore) {
        return errorScore * (((double)frequency + 1) / ((double)dictSize +1));
    }
    
    protected long thresholdFrequency(long termFrequency, long dictionarySize) {
        if (termFrequency > 0) {
            return (long) Math.round(termFrequency * (Math.log10(termFrequency - frequencyPlateau) * (1.0 / Math.log10(logBase))) + 1);
        }
        return 0;
        
    }

}
