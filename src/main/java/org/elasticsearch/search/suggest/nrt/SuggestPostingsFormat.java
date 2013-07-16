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



import org.elasticsearch.search.suggest.nrt.SuggestTokenFilter.ToFiniteStrings;

import org.apache.lucene.codecs.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.index.mapper.FieldMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

/**
 *
 */
public class SuggestPostingsFormat extends PostingsFormat {

    public static final String CODEC_NAME = "suggest";
    public static final int SUGGEST_CODEC_VERSION = 1;
    public static final String EXTENSION = "sgst";

    private PostingsFormat delegatePostingsFormat;
    private SuggestLookupProvider provider = new AnalyzingSuggestLookupProvider(true, 256, -1, false);

    public SuggestPostingsFormat(PostingsFormat delegatePostingsFormat, SuggestLookupProvider provider) {
        super(CODEC_NAME);
        this.delegatePostingsFormat = delegatePostingsFormat;
        this.provider = provider;
    }

    /*
     * Used only by core Lucene at read-time via Service Provider instantiation
     * do not use at Write-time in application code.
     */
    public SuggestPostingsFormat() {
        super(CODEC_NAME);
    }

    @Override
    public SuggestFieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        if (delegatePostingsFormat == null) {
            throw new UnsupportedOperationException("Error - " + getClass().getName()
                    + " has been constructed without a choice of PostingsFormat");
        }
        return new SuggestFieldsConsumer(state);
    }

    @Override
    public SuggestFieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new SuggestFieldsProducer(state);
    }

    private class SuggestFieldsConsumer extends FieldsConsumer {

        private FieldsConsumer delegatesFieldsConsumer;
        private FieldsConsumer suggestFieldsConsumer;

        public SuggestFieldsConsumer(SegmentWriteState state) throws IOException {
            this.delegatesFieldsConsumer = delegatePostingsFormat.fieldsConsumer(state);

            String suggestFSTFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
            IndexOutput output = null;
            boolean success = false;
            try {
                output = state.directory.createOutput(suggestFSTFile, state.context);
                CodecUtil.writeHeader(output, CODEC_NAME, SUGGEST_CODEC_VERSION);
                /*
                 * we write the delegate postings format name so we can load it
                 * without getting an instance in the ctor
                 */
                output.writeString(delegatePostingsFormat.getName());
                output.writeString(provider.getName());
                this.suggestFieldsConsumer = provider.consumer(output);
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(output);
                }
            }
        }

        @Override
        public TermsConsumer addField(final FieldInfo field) throws IOException {
            final TermsConsumer delegateConsumer = delegatesFieldsConsumer.addField(field);
            final TermsConsumer suggestTermConsumer = suggestFieldsConsumer.addField(field);

            return new TermsConsumer() {
                @Override
                public PostingsConsumer startTerm(BytesRef text) throws IOException {
                    final PostingsConsumer suggestPostingsConsumer = suggestTermConsumer.startTerm(text);
                    final PostingsConsumer delegatePostingsConsumer = delegateConsumer.startTerm(text);
                    return new PostingsConsumer() {
                        @Override
                        public void startDoc(int docID, int freq) throws IOException {
                            suggestPostingsConsumer.startDoc(docID, freq);
                            delegatePostingsConsumer.startDoc(docID, freq);
                        }

                        @Override
                        public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
                            suggestPostingsConsumer.addPosition(position, payload, startOffset, endOffset);
                            delegatePostingsConsumer.addPosition(position, payload, startOffset, endOffset);
                        }

                        @Override
                        public void finishDoc() throws IOException {
                            suggestPostingsConsumer.finishDoc();
                            delegatePostingsConsumer.finishDoc();
                        }
                    };
                }

                @Override
                public Comparator<BytesRef> getComparator() throws IOException {
                    return delegateConsumer.getComparator();
                }

                @Override
                public void finishTerm(BytesRef text, TermStats stats) throws IOException {
                    suggestTermConsumer.finishTerm(text, stats);
                    delegateConsumer.finishTerm(text, stats);
                }

                @Override
                public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
                    suggestTermConsumer.finish(sumTotalTermFreq, sumDocFreq, docCount);
                    delegateConsumer.finish(sumTotalTermFreq, sumDocFreq, docCount);
                }
            };
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(delegatesFieldsConsumer, suggestFieldsConsumer);
        }
    }

    private class SuggestFieldsProducer extends FieldsProducer {

        private FieldsProducer delegateProducer;
        private LookupFactory lookupFactory;

        public SuggestFieldsProducer(SegmentReadState state) throws IOException {
            String suggestFSTFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
            IndexInput input = state.directory.openInput(suggestFSTFile, state.context);
            CodecUtil.checkHeader(input, CODEC_NAME, SUGGEST_CODEC_VERSION, SUGGEST_CODEC_VERSION);
            boolean success = false;
            try {
                PostingsFormat delegatePostingsFormat = PostingsFormat.forName(input.readString());
                input.readString(); // NOCOMMIT Lookup the provider
                this.delegateProducer = delegatePostingsFormat.fieldsProducer(state);
                /*
                 * If se are merging we don't load the FSTs at all such that we
                 * don't consum so much memory during merge
                 */
                if (state.context.context != Context.MERGE) {
                    this.lookupFactory = provider.load(input);
                }
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(delegateProducer, input);
                } else {
                    IOUtils.close(input);
                }
            }
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(delegateProducer);
        }

        @Override
        public Iterator<String> iterator() {
            return delegateProducer.iterator();
        }

        @Override
        public Terms terms(String field) throws IOException {
            Terms terms = delegateProducer.terms(field);
            if (terms == null) {
                return terms;
            }
            return new SuggestTerms(terms, this.lookupFactory);
        }

        @Override
        public int size() {
            return delegateProducer.size();
        }
    }

    public static final class SuggestTerms extends Terms {
        private final Terms delegate;
        private final LookupFactory lookup;

        public SuggestTerms(Terms delegate, LookupFactory lookup) {
            this.lookup = lookup;
            this.delegate = delegate;
        }

        @Override
        public TermsEnum iterator(TermsEnum reuse) throws IOException {
            return delegate.iterator(reuse);
        }

        @Override
        public Comparator<BytesRef> getComparator() {
            return delegate.getComparator();
        }

        @Override
        public long size() throws IOException {
            return delegate.size();
        }

        @Override
        public long getSumTotalTermFreq() throws IOException {
            return delegate.getSumTotalTermFreq();
        }

        @Override
        public long getSumDocFreq() throws IOException {
            return delegate.getSumDocFreq();
        }

        @Override
        public int getDocCount() throws IOException {
            return delegate.getDocCount();
        }

        @Override
        public boolean hasOffsets() {
            return delegate.hasOffsets();
        }

        @Override
        public boolean hasPositions() {
            return delegate.hasPositions();
        }

        @Override
        public boolean hasPayloads() {
            return delegate.hasPayloads();
        }

        public Lookup getLookup(FieldMapper<?> mapper, boolean exactFirst) {
            return lookup.getLookup(mapper, exactFirst);
        }

    }

    public static abstract class SuggestLookupProvider implements PayloadProcessor, ToFiniteStrings{
        
        public abstract FieldsConsumer consumer(IndexOutput output);

        public abstract String getName();

        public abstract LookupFactory load(IndexInput input) throws IOException;
        
        @Override
        public BytesRef buildPayload(BytesRef surfaceForm, long weight, BytesRef payload) throws IOException {
            if (weight < -1 || weight > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("weight must be >= -1 && <= Integer.MAX_VALUE");
            }
            for (int i = 0; i < surfaceForm.length; i++) {
                if (surfaceForm.bytes[i] == '\u001f') {
                    throw new IllegalArgumentException(
                            "surface form cannot contain unit separator character U+001F; this character is reserved");
                }
            }
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            OutputStreamDataOutput output = new OutputStreamDataOutput(byteArrayOutputStream);
            output.writeVLong(weight+1);
            output.writeVInt(surfaceForm.length);
            output.writeBytes(surfaceForm.bytes, surfaceForm.offset, surfaceForm.length);
            output.writeVInt(payload.length);
            output.writeBytes(payload.bytes, 0, payload.length);
           
            output.close();
            return new BytesRef(byteArrayOutputStream.toByteArray());
        }
        
        @Override
        public void parsePayload(BytesRef payload, SuggestPayload ref) throws IOException {
            ByteArrayInputStream byteArrayOutputStream = new ByteArrayInputStream(payload.bytes, payload.offset, payload.length);
            InputStreamDataInput input = new InputStreamDataInput(byteArrayOutputStream);
            ref.weight = input.readVLong()-1;
            int len = input.readVInt();
            ref.surfaceForm.grow(len);
            ref.surfaceForm.length = len;
            input.readBytes(ref.surfaceForm.bytes, ref.surfaceForm.offset, ref.surfaceForm.length);
            len = input.readVInt();
            ref.payload.grow(len);
            ref.payload.length = len;
            input.readBytes(ref.payload.bytes, ref.payload.offset, ref.payload.length);
            input.close();
        }
    }

    public static abstract class LookupFactory {
        public abstract Lookup getLookup(FieldMapper<?> mapper, boolean exactFirst);
    }
}
