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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.suggest.FuzzySuggest.FuzzySuggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;

/**
 * Top level suggest result, containing the result for each suggestion.
 */
public class Suggest implements Iterable<Suggest.Suggestion<? extends Entry<? extends Option>>>, Streamable, ToXContent {

    static class Fields {
        static final XContentBuilderString SUGGEST = new XContentBuilderString("suggest");
    }

    private static final Comparator<Option> COMPARATOR = new Comparator<Suggest.Suggestion.Entry.Option>() {
        @Override
        public int compare(Option first, Option second) {
            int cmp = Float.compare(second.getScore(), first.getScore());
            if (cmp != 0) {
                return cmp;
            }
            return first.getText().compareTo(second.getText());
         }
    };

    private Map<String, Suggestion<? extends Entry<? extends Option>>> suggestions;

    public Suggest() {
    }

    public Suggest(Map<String, Suggestion<? extends Entry<? extends Option>>> suggestions) {
        this.suggestions = suggestions;
    }
    
    @Override
    public Iterator<Suggestion<? extends Entry<? extends Option>>> iterator() {
        return suggestions.values().iterator();
    }
    
    public Map<String, Suggestion<? extends Entry<? extends Option>>> getSuggestionMap() {
        return suggestions;
    }
    
    /**
     * The number of suggestions in this {@link Suggest} result
     */
    public int size() {
        return suggestions.size();
    }
    
    public <T extends Suggestion<? extends Entry<? extends Option>>> T getSuggestion(String name) {
        return (T) suggestions.get(name);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        suggestions = new HashMap<String, Suggestion<? extends Entry<? extends Option>>>(size);
        for (int i = 0; i < size; i++) {
            Suggestion<? extends Entry<? extends Option>> suggestion;
            int type = in.readVInt();
            switch (type) {
            case FuzzySuggestion.TYPE:
                suggestion = new FuzzySuggestion();
                break;
            default:
                suggestion = new Suggestion<Entry<Option>>();
                break;
            }
            suggestion.readFrom(in);
            suggestions.put(suggestion.getName(), suggestion);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(suggestions.size());
        for (Suggestion<?> command : suggestions.values()) {
            out.writeVInt(command.getType());
            command.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SUGGEST);
        for (Suggestion<?> suggestion : suggestions.values()) {
            suggestion.toXContent(builder, params);
        }
        builder.endObject();
        return null;
    }

    public static Suggest readSuggest(StreamInput in) throws IOException {
        Suggest result = new Suggest();
        result.readFrom(in);
        return result;
    }

    /**
     * The suggestion responses corresponding with the suggestions in the request.
     */
    public static class Suggestion<T extends Suggestion.Entry> implements Iterable<T>, Streamable, ToXContent {
        
        
        public static final int TYPE = 0;
        protected String name;
        protected int size;
        protected final List<T> entries = new ArrayList<T>(5);

        Suggestion() {
        }

        Suggestion(String name, int size) {
            this.name = name;
            this.size = size; // The suggested term size specified in request, only used for merging shard responses
        }

        void addTerm(T entry) {
            entries.add(entry);
        }
        
        public int getType() {
            return TYPE;
        }

        @Override
        public Iterator<T> iterator() {
            return entries.iterator();
        }

        /**
         * @return The entries for this suggestion.
         */
        public List<T> getEntries() {
            return entries;
        }

        /**
         * @return The name of the suggestion as is defined in the request.
         */
        public String getName() {
            return name;
        }

        /**
         * Merges the result of another suggestion into this suggestion.
         * For internal usage.
         */
        public void reduce(Suggestion<T> other) {
            assert name.equals(other.name);
            assert other.entries.size() == entries.size();
            final Comparator<Option> sortComparator = sortComparator();
            if (entries.size() == 0) {
                return;
            }
            Map<T, T> theseEntries = new HashMap<T, T>();
            for (T t : entries) {
                theseEntries.put(t, t);
            }
            for (T t : other.entries) {
                T merger = theseEntries.get(t);
                if (merger == null) {
                    entries.add(t);
                } else {
                    merger.reduce(t, sortComparator);
                }
            }
        }
        
        protected Comparator<Option> sortComparator() {
            return COMPARATOR;
        }
        
        /**
         * Trims the number of options per suggest text term to the requested size.
         * For internal usage.
         */
        public void trim() {
            for (Entry entry : entries) {
                entry.trim(size);
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            innerReadFrom(in);
            int size = in.readVInt();
            entries.clear();
            for (int i = 0; i < size; i++) {
                T newEntry = newEntry();
                newEntry.readFrom(in);
                entries.add(newEntry);
            }
        }
        
        protected T newEntry() {
            return (T) new Entry();
        }

        
        protected void innerReadFrom(StreamInput in) throws IOException {
            name = in.readString();
            size = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            innerWriteTo(out);
            out.writeVInt(entries.size());
            for (Entry entry : entries) {
                entry.writeTo(out);
            }
        }

        public void innerWriteTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVInt(size);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray(name);
            for (Entry<?> entry : entries) {
                entry.toXContent(builder, params);
            }
            builder.endArray();
            return builder;
        }


        /**
         * Represents a part from the suggest text with suggested options.
         */
        public static class Entry<O extends Entry.Option> implements Iterable<O>, Streamable, ToXContent {

            static class Fields {

                static final XContentBuilderString TEXT = new XContentBuilderString("text");
                static final XContentBuilderString OFFSET = new XContentBuilderString("offset");
                static final XContentBuilderString LENGTH = new XContentBuilderString("length");
                static final XContentBuilderString OPTIONS = new XContentBuilderString("options");

            }

            protected Text text;
            protected int offset;
            protected int length;

            protected List<O> options;

            Entry(Text text, int offset, int length) {
                this.text = text;
                this.offset = offset;
                this.length = length;
                this.options = new ArrayList<O>(5);
            }

            Entry() {
            }

            void addOption(O option) {
                options.add(option);
            }

            void reduce(Entry<O> otherEntry, Comparator<O> comparator) {
                assert text.equals(otherEntry.text);
                assert offset == otherEntry.offset;
                assert length == otherEntry.length;
                if (this.options.isEmpty()) {
                    this.options = otherEntry.options;
                    return;
                } else if (otherEntry.options.isEmpty()) {
                    return;
                }
                
                final Map<O, O> theseEntries = new HashMap<O, O>();
                for (O o : this.options) {
                    theseEntries.put(o, o);
                }
                for (O otherOption : otherEntry.options) {
                   O merger = theseEntries.get(otherOption);
                   if (merger == null) {
                      this.options.add(otherOption);
                   } else {
                       merger.mergeInto(otherOption);
                   }
                }
                Collections.sort(options, comparator);
            }
            
            /**
             * @return the text (analyzed by suggest analyzer) originating from the suggest text. Usually this is a
             *         single term.
             */
            public Text getText() {
                return text;
            }

            /**
             * @return the start offset (not analyzed) for this entry in the suggest text.
             */
            public int getOffset() {
                return offset;
            }

            /**
             * @return the length (not analyzed) for this entry in the suggest text.
             */
            public int getLength() {
                return length;
            }

            @Override
            public Iterator<O> iterator() {
                return options.iterator();
            }

            /**
             * @return The suggested options for this particular suggest entry. If there are no suggested terms then
             *         an empty list is returned.
             */
            public List<O> getOptions() {
                return options;
            }

            void trim(int size) {
                int optionsToRemove = Math.max(0, options.size() - size);
                for (int i = 0; i < optionsToRemove; i++) {
                    options.remove(options.size() - 1);
                }
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Entry<?> entry = (Entry<?>) o;

                if (length != entry.length) return false;
                if (offset != entry.offset) return false;
                if (!this.text.equals(entry.text)) return false;

                return true;
            }

            @Override
            public int hashCode() {
                int result = text.hashCode();
                result = 31 * result + offset;
                result = 31 * result + length;
                return result;
            }

            @Override
            public void readFrom(StreamInput in) throws IOException {
                text = in.readText();
                offset = in.readVInt();
                length = in.readVInt();
                int suggestedWords = in.readVInt();
                options = new ArrayList<O>(suggestedWords);
                for (int j = 0; j < suggestedWords; j++) {
                    O newOption = newOption();
                    newOption.readFrom(in);
                    options.add(newOption);
                }
            }
            
            protected O newOption(){
                return (O) new Option();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeText(text);
                out.writeVInt(offset);
                out.writeVInt(length);
                out.writeVInt(options.size());
                for (Option option : options) {
                    option.writeTo(out);
                }
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(Fields.TEXT, text);
                builder.field(Fields.OFFSET, offset);
                builder.field(Fields.LENGTH, length);
                builder.startArray(Fields.OPTIONS);
                for (Option option : options) {
                    option.toXContent(builder, params);
                }
                builder.endArray();
                builder.endObject();
                return builder;
            }

            /**
             * Contains the suggested text with its document frequency and score.
             */
            public static class Option implements Streamable, ToXContent {

                static class Fields {

                    static final XContentBuilderString TEXT = new XContentBuilderString("text");
                    static final XContentBuilderString SCORE = new XContentBuilderString("score");

                }

                private Text text;
                private float score;

                Option(Text text, float score) {
                    this.text = text;
                    this.score = score;
                }

                Option() {
                }

                /**
                 * @return The actual suggested text.
                 */
                public Text getText() {
                    return text;
                }

                /**
                 * @return The score based on the edit distance difference between the suggested term and the
                 *         term in the suggest text.
                 */
                public float getScore() {
                    return score;
                }

                @Override
                public void readFrom(StreamInput in) throws IOException {
                    text = in.readText();
                    score = in.readFloat();
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    out.writeText(text);
                    out.writeFloat(score);
                }

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    builder.startObject();
                    innerToXContent(builder, params);
                    builder.endObject();
                    return builder;
                }
                
                protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
                    builder.field(Fields.TEXT, text);
                    builder.field(Fields.SCORE, score);
                    return builder;
                }
                
                protected void mergeInto(Option otherOption) {
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;

                    Option that = (Option) o;
                    return text.equals(that.text);

                }

                @Override
                public int hashCode() {
                    return text.hashCode();
                }
            }

        }
        
        enum Sort {

            /**
             * Sort should first be based on score.
             */
            SCORE((byte) 0x0),

            /**
             * Sort should first be based on document frequency.
             */
            FREQUENCY((byte) 0x1);

            private byte id;

            private Sort(byte id) {
                this.id = id;
            }

            public byte id() {
                return id;
            }

            static Sort fromId(byte id) {
                if (id == 0) {
                    return SCORE;
                } else if (id == 1) {
                    return FREQUENCY;
                } else {
                    throw new ElasticSearchException("Illegal suggest sort " + id);
                }
            }

        }

    }

}
