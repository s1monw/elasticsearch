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
import java.util.Comparator;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.suggest.FuzzySuggest.FuzzySuggestion.FuzzyEntry.FuzzyOption;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;

public class FuzzySuggest {
    
    public static Comparator<Suggestion.Entry.Option> SCORE = new Score();
    public static Comparator<Suggestion.Entry.Option> FREQUENCY = new Frequency();

    // Same behaviour as comparators in suggest module, but for SuggestedWord
    // Highest score first, then highest freq first, then lowest term first
    public static class Score implements Comparator<Suggestion.Entry.Option> {

        @Override
        public int compare(Suggestion.Entry.Option first, Suggestion.Entry.Option second) {
            // first criteria: the distance
            int cmp = Float.compare(second.getScore(), first.getScore());
            if (cmp != 0) {
                return cmp;
            }
            return FREQUENCY.compare(first, second);
        }

    }

    // Same behaviour as comparators in suggest module, but for SuggestedWord
    // Highest freq first, then highest score first, then lowest term first
    public static class Frequency implements Comparator<Suggestion.Entry.Option> {

        @Override
        public int compare(Suggestion.Entry.Option first, Suggestion.Entry.Option second) {
            
            // first criteria: the popularity
            int cmp = ((FuzzyOption)second).getFreq() - ((FuzzyOption)first).getFreq();
            if (cmp != 0) {
                return cmp;
            }

            // second criteria (if first criteria is equal): the distance
            cmp = Float.compare(second.getScore(), first.getScore());
            if (cmp != 0) {
                return cmp;
            }

            // third criteria: term text
            return first.getText().compareTo(second.getText());
        }

    }


    /**
     * The suggestion responses corresponding with the suggestions in the request.
     */
    public static class FuzzySuggestion extends Suggestion<FuzzySuggestion.FuzzyEntry> {

        public static final int TYPE = 1;
        private Sort sort;

        FuzzySuggestion() {
        }

        FuzzySuggestion(String name, int size, Sort sort) {
            super(name, size);
            this.sort = sort;
        }

        public int getType() {
            return TYPE;
        }

        
        @Override
        protected Comparator<Option> sortComparator() {
            switch (sort) {
            case SCORE:
                return SCORE;
            case FREQUENCY:
                return FREQUENCY;
            default:
                throw new ElasticSearchException("Could not resolve comparator for sort key: [" + sort + "]");
            }
        }


        @Override
        protected void innerReadFrom(StreamInput in) throws IOException {
            super.innerReadFrom(in);
            sort = Sort.fromId(in.readByte());
        }

        @Override
        public void innerWriteTo(StreamOutput out) throws IOException {
            super.innerWriteTo(out);
            out.writeByte(sort.id());
        }
        
        protected FuzzyEntry newEntry() {
            return new FuzzyEntry();
        }


        /**
         * Represents a part from the suggest text with suggested options.
         */
        public static class FuzzyEntry extends org.elasticsearch.search.suggest.Suggest.Suggestion.Entry<FuzzySuggestion.FuzzyEntry.FuzzyOption>{

            FuzzyEntry(Text text, int offset, int length) {
               super(text, offset, length);
            }

            FuzzyEntry() {
            }

            @Override
            protected FuzzyOption newOption() {
               return new FuzzyOption();
            }

            /**
             * Contains the suggested text with its document frequency and score.
             */
            public static class FuzzyOption extends Option {

                static class Fields {
                    static final XContentBuilderString FREQ = new XContentBuilderString("freq");
                }
                
                private int freq;

                protected FuzzyOption(Text text, int freq, float score) {
                    super(text, score);
                    this.freq = freq;
                }

                @Override
                protected void mergeInto(Option otherOption) {
                    super.mergeInto(otherOption);
                    freq += ((FuzzyOption)otherOption).freq;
                }

                protected FuzzyOption() {
                    super();
                }

                public void setFreq(int freq) {
                    this.freq = freq;
                }

                /**
                 * @return How often this suggested text appears in the index.
                 */
                public int getFreq() {
                    return freq;
                }

                @Override
                public void readFrom(StreamInput in) throws IOException {
                    super.readFrom(in);
                    freq = in.readVInt();
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    super.writeTo(out);
                    out.writeVInt(freq);
                }

                @Override
                protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
                    builder = super.innerToXContent(builder, params);
                    builder.field(Fields.FREQ, freq);
                    return builder;
                }
            }

        }
    }

}
