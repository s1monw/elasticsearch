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
package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenFilter;

import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;

import org.apache.lucene.store.InputStreamDataInput;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
            posInc = 256 - strings.size();
            finiteStrings = strings.iterator();
        }
        BytesRef scratch = new BytesRef();
        if (finiteStrings.hasNext()) {
            posAttr.setPositionIncrement(posInc);
            /*
             *  this posInc encodes the number of paths that this surface form produced.
             *  256 is the upper bound in the anayzing suggester so we simply don't allow more that that.
             *  TODO check that we don't have more that that - maybe throw an exception & add a test.
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
    
    

    /*
    public static interface SuggestAttribute extends Attribute, TermToBytesRefAttribute {
        String[] suggest(String query);

        void setSurfaceForm(BytesRef surfaceForm);

        void setPayload(BytesRef payload);

        void setWeight(int weight);

        void setAutomaton(BytesRef automaton);
    }

    public static class SuggestAttributeImpl extends AttributeImpl implements SuggestAttribute {

        private static int MIN_BUFFER_SIZE = 10;
        private BytesRef payload;
        private BytesRef surfaceForm;
        private int weight;
        private BytesRef automaton;

        @Override
        public void clear() {
            //throw new ElasticSearchException("not yet implemented");
        }

        @Override
        public void copyTo(AttributeImpl target) {
            throw new ElasticSearchException("not yet implemented");
        }

        @Override
        public String[] suggest(String query) {
            // TODO: check the automaton
            // TODO: execute some query?!
            // TODO: include weight in automaton query?!
            // TODO: include payload

            if (surfaceForm.isValid()) {
                return new String[] { surfaceForm.utf8ToString() };
            }
            return new String[]{};
        }

        @Override
        public void setSurfaceForm(BytesRef surfaceForm) {
            this.surfaceForm = surfaceForm;
        }

        @Override
        public void setPayload(BytesRef payload) {
            this.payload = payload;
        }

        @Override
        public void setWeight(int weight) {
            this.weight = weight;
        }

        @Override
        public void setAutomaton(BytesRef automaton) {
            this.automaton = automaton;
        }

        private BytesRef bytes = new BytesRef(MIN_BUFFER_SIZE);

        // not until java 6 @Override
        @Override
        public int fillBytesRef() {
            return 0;//UnicodeUtil.UTF16toUTF8WithHash(termBuffer, 0, termLength, bytes);
        }

        // not until java 6 @Override
        @Override
        public BytesRef getBytesRef() {
            return bytes;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (surfaceForm != null) sb.append(surfaceForm.utf8ToString());
            if (payload != null) sb.append("/payload " + payload.utf8ToString());
            sb.append("/weight " + weight );
            return sb.toString();
        }
    }
    */

    /*
    static {
        try {
            AtomicReader reader = null;
            Terms terms = reader.fields().terms("my_suggest_field");
            TermsEnum iterator = terms.iterator(null);
            SuggestAttribute suggest = iterator.attributes().addAttribute(SuggestAttribute.class);
            suggest.suggest("foo");
        } catch (Exception e) {
        }
    }
    */
}