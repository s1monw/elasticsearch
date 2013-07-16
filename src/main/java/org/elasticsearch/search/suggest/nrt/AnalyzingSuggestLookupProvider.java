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

import org.elasticsearch.search.suggest.nrt.SuggestPostingsFormat.LookupFactory;
import org.elasticsearch.search.suggest.nrt.SuggestPostingsFormat.SuggestLookupProvider;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.*;
import org.apache.lucene.util.fst.PairOutputs.Pair;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.SuggestFieldMapper;
import org.elasticsearch.index.mapper.core.SuggestFieldMapper.SuggestPayload;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class AnalyzingSuggestLookupProvider extends SuggestLookupProvider {

    private boolean preserveSep;
    private int maxSurfaceFormsPerAnalyzedForm;
    private int maxGraphExpansions;
    private boolean hasPayloads;

    public AnalyzingSuggestLookupProvider(boolean preserveSep, int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions,
            boolean hasPayloads) {
        this.preserveSep = preserveSep;
        this.maxSurfaceFormsPerAnalyzedForm = maxSurfaceFormsPerAnalyzedForm;
        this.maxGraphExpansions = maxGraphExpansions;
        this.hasPayloads = hasPayloads;
    }

    @Override
    public String getName() {
        return "analyzing";
    }

    @Override
    public FieldsConsumer consumer(final IndexOutput output) {
        return new FieldsConsumer() {
            private Map<FieldInfo, Long> fieldOffsets = new HashMap<FieldInfo, Long>();
            @Override
            public void close() throws IOException {
                try { /*
                       * write the offsets per field such that we know where
                       * we need to load the FSTs from
                       */
                    long pointer = output.getFilePointer();
                    output.writeVInt(fieldOffsets.size());
                    for (Map.Entry<FieldInfo, Long> entry : fieldOffsets.entrySet()) {
                        output.writeString(entry.getKey().name);
                        output.writeVLong(entry.getValue());
                    }
                    output.writeLong(pointer);
                    output.flush();
                } finally {
                    IOUtils.close(output);
                }
            }

            @Override
            public TermsConsumer addField(final FieldInfo field) throws IOException {

                return new TermsConsumer() {
                    final XAnalyzingSuggester.XBuilder builder = new XAnalyzingSuggester.XBuilder(maxSurfaceFormsPerAnalyzedForm,
                            hasPayloads);
                    final SuggestPayload spare = new SuggestPayload();
                    int maxAnalyzedPathsForOneInput = 0;

                    @Override
                    public PostingsConsumer startTerm(BytesRef text) throws IOException {
                        builder.startTerm(text);
                        return new PostingsConsumer() { // TODO reuse
                            @Override
                            public void startDoc(int docID, int freq) throws IOException {
                            }

                            @Override
                            public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
                                SuggestFieldMapper.DEFAULT_PROCESSOR.parsePayload(payload, spare);
                                builder.addSurface(spare.surfaceForm, spare.payload, spare.weight);
                                /*
                                 * This is tricky
                                 * maxAnalyzedPathsForOneInput is the max
                                 * number of paths that a single input in
                                 * the tokenstream produced. this is bounded
                                 * by 256 in the suggester - we simply don't
                                 * allow more. Now we need to know the max
                                 * number of paths we have seen during
                                 * analysis so we use (256 - numPath) as the
                                 * initial position increment such that we
                                 * can just use modulo to get back the max
                                 * num of paths. See SuggestTokenStream
                                 */
                                maxAnalyzedPathsForOneInput = Math.max(maxAnalyzedPathsForOneInput, 256 - (position % 256));
                                /*
                                 * NOCOMMIT what are we doing if no weights
                                 * are present? - We might want to have a
                                 * simple version of this class using a
                                 * WFSTCompletionLookup that only uses
                                 * frequencies?
                                 */
                            }

                            @Override
                            public void finishDoc() throws IOException {
                            }
                        };
                    }

                    @Override
                    public Comparator<BytesRef> getComparator() throws IOException {
                        return null;
                    }

                    @Override
                    public void finishTerm(BytesRef text, TermStats stats) throws IOException {
                        builder.finishTerm();
                    }

                    @Override
                    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
                        /*
                         * Here we are done processing the field and we can
                         * buid the FST and write it to disk.
                         */
                        FST<Pair<Long, BytesRef>> build = builder.build();
                        fieldOffsets.put(field, output.getFilePointer());
                        build.save(output);
                        /* write some more meta-info */
                        output.writeVInt(maxAnalyzedPathsForOneInput);
                        output.writeVInt(maxSurfaceFormsPerAnalyzedForm);
                        output.writeInt(maxGraphExpansions); // can be
                                                             // negative
                        output.writeByte((byte) (preserveSep ? 1 : 0));
                        output.writeByte((byte) (hasPayloads ? 1 : 0));
                    }
                };
            }
        };
    }

    @Override
    public LookupFactory load(IndexInput input) throws IOException {
        final Map<String, AnalyzingSuggestHolder> lookupMap = new HashMap<String, AnalyzingSuggestHolder>();
        input.seek(input.length() - 8);
        long metaPointer = input.readLong();
        input.seek(metaPointer);
        int numFields = input.readVInt();
        /*
         * NOCOMMIT we should load the metadata and then sort them by offset
         * so we can load the FSTs sequentially. We might want to load them
         * lazly but that is yet another sync point that I'd wanna safe?
         */
        Map<String, Long> meta = new HashMap<String, Long>();
        for (int i = 0; i < numFields; i++) {
            String name = input.readString();
            long offset = input.readVLong();
            meta.put(name, offset);
        }
        for (Map.Entry<String, Long> entry : meta.entrySet()) {
            input.seek(entry.getValue());
            FST<Pair<Long, BytesRef>> fst = new FST<Pair<Long, BytesRef>>(input, new PairOutputs<Long, BytesRef>(
                    PositiveIntOutputs.getSingleton(true), ByteSequenceOutputs.getSingleton()));
            int maxAnalyzedPathsForOneInput = input.readVInt();
            int maxSurfaceFormsPerAnalyzedForm = input.readVInt();
            int maxGraphExpansions = input.readInt();
            boolean preserveSep = input.readByte() == 1;
            boolean hasPayloads = input.readByte() == 1;
            lookupMap.put(entry.getKey(), new AnalyzingSuggestHolder(preserveSep, maxSurfaceFormsPerAnalyzedForm, maxGraphExpansions,
                    hasPayloads, maxAnalyzedPathsForOneInput, fst));
        }
        return new LookupFactory() {
            @Override
            public Lookup getLookup(FieldMapper<?> mapper, boolean exactFirst) {
                AnalyzingSuggestHolder analyzingSuggestHolder = lookupMap.get(mapper.names().name());
                if (analyzingSuggestHolder == null) {
                    return null;
                }
                int flags = exactFirst ? 0 : XAnalyzingSuggester.EXACT_FIRST;
                flags = analyzingSuggestHolder.preserveSep ? 0 : XAnalyzingSuggester.PRESERVE_SEP;
                return new XAnalyzingSuggester(mapper.indexAnalyzer(), mapper.searchAnalyzer(), flags,
                        analyzingSuggestHolder.maxSurfaceFormsPerAnalyzedForm, analyzingSuggestHolder.maxGraphExpansions,
                        analyzingSuggestHolder.fst, analyzingSuggestHolder.hasPayloads,
                        analyzingSuggestHolder.maxAnalyzedPathsForOneInput);
            }
        };
    }
    
    static class AnalyzingSuggestHolder {
        final boolean preserveSep;
        final int maxSurfaceFormsPerAnalyzedForm;
        final int maxGraphExpansions;
        final boolean hasPayloads;
        final int maxAnalyzedPathsForOneInput;
        final FST<Pair<Long, BytesRef>> fst;

        public AnalyzingSuggestHolder(boolean preserveSep, int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions, boolean hasPayloads,
                int maxAnalyzedPathsForOneInput, FST<Pair<Long, BytesRef>> fst) {
            this.preserveSep = preserveSep;
            this.maxSurfaceFormsPerAnalyzedForm = maxSurfaceFormsPerAnalyzedForm;
            this.maxGraphExpansions = maxGraphExpansions;
            this.hasPayloads = hasPayloads;
            this.maxAnalyzedPathsForOneInput = maxAnalyzedPathsForOneInput;
            this.fst = fst;
        }

    }


}