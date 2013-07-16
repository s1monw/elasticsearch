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
package org.elasticsearch.index.mapper.core;

import org.elasticsearch.search.suggest.nrt.SuggestPostingsFormatProvider;

import org.elasticsearch.search.suggest.nrt.AnalyzingSuggestLookupProvider;




import org.apache.lucene.analysis.Analyzer;

import org.elasticsearch.ElasticSearchIllegalArgumentException;

import com.google.common.collect.Lists;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.SuggestTokenFilter;
import org.elasticsearch.index.analysis.SuggestTokenFilter.ToFiniteStrings;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class SuggestFieldMapper extends AbstractFieldMapper<String> {

    public static final String CONTENT_TYPE = "suggest";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends AbstractFieldMapper.OpenBuilder<Builder, SuggestFieldMapper>  {

        private NamedAnalyzer searchAnalyzer;
        private NamedAnalyzer indexAnalyzer;
        private String suggester;
        private ContentPath.Type pathType;

        public Builder(String name) {
            super(name, FIELD_TYPE);
        }

        public Builder searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return this;
        }

        public Builder indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            return this;
        }

        public Builder suggester(String suggester) {
            this.suggester = suggester;
            return this;
        }

        public Builder pathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        @Override
        public SuggestFieldMapper build(Mapper.BuilderContext context) {
            return new SuggestFieldMapper(buildNames(context), pathType, indexAnalyzer, searchAnalyzer, suggester, provider, similarity);
        }
    }

    /*
    "myFooField" {
        "type" : "suggest"
        "index_analyzer" : "stopword",
        "search_analyzer" : "simple",
        "suggester" : "analyzing_prefix"
    }
    */
    /*
     * NOCOMMIT - we need to know if we have payloads or not ahead of time. we might wanna have something like suggest_options or flags like:
     * "payloads" : true|false
     * ...
     * "frequency_weights" : true|false
     * 
     */
    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            SuggestFieldMapper.Builder builder = new SuggestFieldMapper.Builder(name);

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();

                if (fieldName.equals("type")) continue;

                if (fieldName.equals("index_analyzer") || fieldName.equals("indexAnalyzer")) {
                    builder.indexAnalyzer(parserContext.analysisService().analyzer(fieldNode.toString()));
                } else if (fieldName.equals("search_analyzer") || fieldName.equals("searchAnalyzer")) {
                    builder.searchAnalyzer(parserContext.analysisService().analyzer(fieldNode.toString()));
                } else if (fieldName.equals("suggester")) {
                    builder.suggester(fieldNode.toString());
                }
            }

            if (builder.searchAnalyzer == null) {
                builder.searchAnalyzer(parserContext.analysisService().defaultSearchAnalyzer());
            }

            if (builder.indexAnalyzer == null) {
                builder.indexAnalyzer(parserContext.analysisService().defaultIndexAnalyzer());
            }
            
            AnalyzingSuggestLookupProvider analyzingSuggestLookupProvider = new AnalyzingSuggestLookupProvider(true, 256, -1, false);
            builder.postingsFormat(new SuggestPostingsFormatProvider("suggest", parserContext.postingFormatService().get("default"), analyzingSuggestLookupProvider)); // TODO ???

            return builder;
        }
    }

    private final String suggester;
    private final AnalyzingSuggestProvider analyzingSuggestProvider;
    public static final PayloadProcessor DEFAULT_PROCESSOR = new DefautlPayloadProcessor(); 

    public SuggestFieldMapper(Names names, ContentPath.Type pathType, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer, String suggester, PostingsFormatProvider provider, SimilarityProvider similarity) {
        super(names, 1.0f, FIELD_TYPE, indexAnalyzer, searchAnalyzer, provider, similarity, null);
        // NOCOMMIT - we might need to configure the PostingsFormatProvider in here and return a custom version once the SuggestFormatProvider has settings on the 
        this.suggester = suggester;
        analyzingSuggestProvider = new AnalyzingSuggestProvider(new XAnalyzingSuggester(indexAnalyzer, searchAnalyzer)); // NOCOMMIT expose settings?
        
    }
    
    

    /*
    "myFooField" : {
        "input" : [ "The Prodigy Firestarter", "Firestarter"],
        "surface_form" : "The Prodigy, Firestarter",
        "weight" : 42,
        "payload" : "whatever"
    }

    "myFooField" {
        "input" : [ "The Prodigy Firestarter", "Firestarter"],
        "surface_form" : "The Prodigy, Firestarter"
    }

    "myFooField" {
        "input" : [ "The Prodigy Firestarter", "Firestarter"]
    }

    "myFooField" : [ "The Prodigy Firestarter", "Firestarter"]
     */
    @Override
    public void parse(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();

        String surfaceForm = null;
        String payload = null;
        long weight = -1;
        List<String> inputs = Lists.newArrayListWithExpectedSize(4);

        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                inputs.add(parser.text());
            }
        } else {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if ("surface_form".equals(currentFieldName)) {
                        surfaceForm = parser.text();
                    } else if ("payload".equals(currentFieldName)) {
                        payload = parser.text();
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if ("weight".equals(currentFieldName)) {
                        weight = parser.longValue(); // always parse a long to make sure we don't get the overflow value
                        if (weight < 0 || weight > Integer.MAX_VALUE) {
                            throw new ElasticSearchIllegalArgumentException("Weight must be in the interval [0..2147483647] but was " + weight);
                        }
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("input".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            inputs.add(parser.text());
                        }
                    }
                }
            }
        }

        // TODO: This is clearly wrong
        for (String input : inputs) {
            Field suggestField = new SuggestField(name(), input, this.fieldType, DEFAULT_PROCESSOR.buildPayload(new BytesRef(surfaceForm), weight,  payload.getBytes()), this.analyzingSuggestProvider);
            context.doc().add(suggestField);
            /*
            StringFieldMapper.StringTokenStream stringTokenStream = new StringFieldMapper.StringTokenStream().setValue(input);
            TokenStream tokenStream = new SuggestTokenFilter(stringTokenStream, surfaceForm, payload.getBytes(), weight);
            Analyzer analyzer = context.analysisService().analyzer(indexAnalyzer.name()).analyzer();
            */

            //Field suggestField = new TextField(name, tokenStream);
            //context.doc().add(new Field(name, tokenStream));
        }
        //Field field = new Field(name, inputs.get(0), new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE));
        //field.tokenStreamValue().addAttributeImpl(new PayloadAttributeImpl(new BytesRef(payload)));
        //field.tokenStreamValue().addAttributeImpl(new NumericTokenStream.NumericTermAttributeImpl());
    }
    
    private static final class SuggestField extends Field {

        private final BytesRef payload;
        private final ToFiniteStrings toFiniteStrings;

        public SuggestField(String name, String value, FieldType type, BytesRef payload, ToFiniteStrings toFiniteStrings) {
            super(name, value, type);
            this.payload = payload;
            this.toFiniteStrings = toFiniteStrings;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer) throws IOException {
            TokenStream ts = super.tokenStream(analyzer);
            return new SuggestTokenFilter(ts, payload, toFiniteStrings);
        }

        
    }
    public static interface PayloadProcessor {
        public BytesRef buildPayload(BytesRef surfaceForm, long weight, byte[] payload) throws IOException;
        public void parsePayload(BytesRef payload, SuggestPayload ref) throws IOException;
    }
    
    public static class SuggestPayload {
        public final BytesRef payload = new BytesRef();
        public long weight = 0;
        public final BytesRef surfaceForm = new BytesRef();
    }
    
    public static class DefautlPayloadProcessor implements PayloadProcessor {
        @Override
        public BytesRef buildPayload(BytesRef surfaceForm, long weight, byte[] payload) throws IOException {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            OutputStreamDataOutput output = new OutputStreamDataOutput(byteArrayOutputStream);
            output.writeVLong(weight);
            output.writeVInt(surfaceForm.length);
            output.writeBytes(surfaceForm.bytes, surfaceForm.offset, surfaceForm.length);
            output.writeVInt(payload.length);
            output.writeBytes(payload, 0, payload.length);
            output.close();
            return new BytesRef(byteArrayOutputStream.toByteArray());
        }
        
        @Override
        public void parsePayload(BytesRef payload, SuggestPayload ref) throws IOException {
            ByteArrayInputStream byteArrayOutputStream = new ByteArrayInputStream(payload.bytes, payload.offset, payload.length);
            InputStreamDataInput input = new InputStreamDataInput(byteArrayOutputStream);
            ref.weight = input.readVLong();
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
    
    public static class AnalyzingSuggestProvider implements ToFiniteStrings{
        private XAnalyzingSuggester suggester;

        public AnalyzingSuggestProvider(XAnalyzingSuggester suggester) {
            this.suggester = suggester;
        }
        @Override
        public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException {
            return suggester.toFiniteStrings(suggester.getTokenStreamToAutomaton(), stream);
        }

      
        
    }
    
   

    public static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setIndexed(true);
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.setStored(true);
        FIELD_TYPE.setStoreTermVectors(false);
        FIELD_TYPE.setOmitNorms(false);
        FIELD_TYPE.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        FIELD_TYPE.freeze();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name());
        builder.field("type", CONTENT_TYPE);
        builder.field("index_analyzer", indexAnalyzer.name());
        builder.field("search_analyzer", searchAnalyzer.name());
        builder.field("suggester", suggester);
        builder.endObject();
        return builder;
    }

    @Override
    protected Field parseCreateField(ParseContext context) throws IOException {
        return null;
    }



    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
    

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("string");
    }


    @Override
    public String value(Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

}
