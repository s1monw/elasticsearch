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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 *
 */
public final class SuggestTokenFilter extends TokenFilter {

    private final CharTermAttribute termAttr;
    private final PayloadAttribute payloadAttr;
    private final PositionIncrementAttribute posAttr;

    private final TokenStream input;
    private BytesRef payload;
    private Iterator<IntsRef> finiteStrings;
    private ToFiniteStrings toFiniteStrings;
    private int posInc;
    private static final int MAX_PATHS = 256;
    private final BytesRef scratch = new BytesRef();


    public SuggestTokenFilter(TokenStream input, BytesRef payload, ToFiniteStrings toFiniteStrings) throws IOException {
        super(input);
        termAttr = addAttribute(CharTermAttribute.class);
        payloadAttr = addAttribute(PayloadAttribute.class);
        posAttr = addAttribute(PositionIncrementAttribute.class);
        this.input = input;
        this.payload = payload;
        this.toFiniteStrings = toFiniteStrings;
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (finiteStrings == null) {
            Set<IntsRef> strings = toFiniteStrings.toFiniteStrings(input);
            
            if (strings.size() > MAX_PATHS) {
                throw new IllegalArgumentException("TokenStream expanded to " + strings.size() + " finite strings. Only <= " + MAX_PATHS
                        + " finite strings are supported");
            }
            posInc = MAX_PATHS - strings.size();
            finiteStrings = strings.iterator();
        }
        if (finiteStrings.hasNext()) {
            posAttr.setPositionIncrement(posInc);
            /*
             *  this posInc encodes the number of paths that this surface form produced.
             *  256 is the upper bound in the anayzing suggester so we simply don't allow more that that.
             *  See SuggestPostingsFormat for more details.
             */
            posInc = 0;
            Util.toBytesRef(finiteStrings.next(), scratch); // now we have UTF-8
            // length of the analyzed text (FST input)
            if (scratch.length > Short.MAX_VALUE-2) {
                throw new IllegalArgumentException("cannot handle analyzed forms > " + (Short.MAX_VALUE-2) + " in length (got " + scratch.length + ")");
            }
            termAttr.setEmpty();
            termAttr.append(scratch.utf8ToString());
            if (payload != null) {
                payloadAttr.setPayload(this.payload);
            }
            return true;
        }

        return false;
    }
    
    public static interface ToFiniteStrings {
        public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException;
    }
    
    @Override
    public void reset() throws IOException {
        super.reset();
        finiteStrings = null;
    }
    
}