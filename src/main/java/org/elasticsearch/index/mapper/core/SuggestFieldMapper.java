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

import com.google.common.collect.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.search.suggest.nrt.AnalyzingSuggestLookupProvider;
import org.elasticsearch.search.suggest.nrt.SuggestPostingsFormatProvider;
import org.elasticsearch.search.suggest.nrt.SuggestTokenFilter;
import org.elasticsearch.search.suggest.nrt.SuggestTokenFilter.ToFiniteStrings;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SuggestFieldMapper extends AbstractFieldMapper<String> {

    public static final String CONTENT_TYPE = "suggest";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends AbstractFieldMapper.OpenBuilder<Builder, SuggestFieldMapper>  {

        private NamedAnalyzer searchAnalyzer;
        private NamedAnalyzer indexAnalyzer;
        private ContentPath.Type pathType;
        private boolean preserveSeparators;
        private boolean payloads;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
        }

        public Builder searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return this;
        }

        public Builder indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            return this;
        }

        public Builder pathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        @Override
        public SuggestFieldMapper build(Mapper.BuilderContext context) {
            return new SuggestFieldMapper(buildNames(context), pathType, indexAnalyzer, searchAnalyzer, provider, similarity, payloads, preserveSeparators);
        }

        public Builder payloads(boolean payloads) {
            this.payloads = payloads;
            return this;
        }

        public Builder preserveSeparators(boolean preserveSeparators) {
            this.preserveSeparators = preserveSeparators;
            return this;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            SuggestFieldMapper.Builder builder = new SuggestFieldMapper.Builder(name);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                
                if (fieldName.equals("type")) {
                    continue;
                }
                if (fieldName.equals("index_analyzer") || fieldName.equals("indexAnalyzer")) {
                    builder.indexAnalyzer(parserContext.analysisService().analyzer(fieldNode.toString()));
                } else if (fieldName.equals("search_analyzer") || fieldName.equals("searchAnalyzer")) {
                    builder.searchAnalyzer(parserContext.analysisService().analyzer(fieldNode.toString()));
                } else if (fieldName.equals("payloads")) {
                    builder.payloads(Boolean.parseBoolean(fieldNode.toString()));
                } else if (fieldName.equals("preserve_separators")) {
                    builder.preserveSeparators(Boolean.parseBoolean(fieldNode.toString()));
                }
            }

            if (builder.searchAnalyzer == null) {
                builder.searchAnalyzer(parserContext.analysisService().defaultSearchAnalyzer());
            }

            if (builder.indexAnalyzer == null) {
                builder.indexAnalyzer(parserContext.analysisService().defaultIndexAnalyzer());
            }
            builder.postingsFormat(parserContext.postingFormatService().get("default"));
            return builder;
        }
    }

    private final SuggestPostingsFormatProvider suggestPostingsFormatProvider;
    private final AnalyzingSuggestLookupProvider analyzingSuggestLookupProvider;
    private final boolean payloads;
    private final boolean preserveSeparators;

    public SuggestFieldMapper(Names names, ContentPath.Type pathType, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer, PostingsFormatProvider provider, SimilarityProvider similarity, boolean payloads, boolean preserveSeparators) {
        super(names, 1.0f, Defaults.FIELD_TYPE, indexAnalyzer, searchAnalyzer, provider, similarity, null);
        analyzingSuggestLookupProvider = new AnalyzingSuggestLookupProvider(preserveSeparators, 256, -1, payloads);
        this.suggestPostingsFormatProvider = new SuggestPostingsFormatProvider("suggest", provider, analyzingSuggestLookupProvider);    
        this.preserveSeparators = preserveSeparators;
        this.payloads = payloads;
    }

    @Override
    public PostingsFormatProvider postingsFormatProvider() {
        return this.suggestPostingsFormatProvider;
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
        String payload = "";
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
        payload = payload == null ? "" : payload;
        if (surfaceForm == null) { // no surface form use the input
            for (String input : inputs) {
                BytesRef suggestPayload = analyzingSuggestLookupProvider.buildPayload(new BytesRef(
                        input), weight, payload.getBytes());
                Field suggestField = new SuggestField(name(), input, this.fieldType, suggestPayload, analyzingSuggestLookupProvider);
                context.doc().add(suggestField);
            }
        } else {
            BytesRef suggestPayload = analyzingSuggestLookupProvider.buildPayload(new BytesRef(
                    surfaceForm), weight, payload.getBytes());
            for (String input : inputs) {
                Field suggestField = new SuggestField(name(), input, this.fieldType, suggestPayload, analyzingSuggestLookupProvider);
                context.doc().add(suggestField);
            }    
        }
        
        
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

    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name());
        builder.field("type", CONTENT_TYPE);
        builder.field("index_analyzer", indexAnalyzer.name());
        builder.field("search_analyzer", searchAnalyzer.name());
        builder.field("payloads", this.payloads);
        builder.field("preserve_separators", this.preserveSeparators);
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
        return null;
    }


    @Override
    public String value(Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

}
