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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.diskdv.DiskDocValuesFormat;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.Map;

import javax.print.DocFlavor.STRING;

import static org.elasticsearch.index.mapper.MapperBuilders.uid;

/**
 *
 */
public class UidFieldMapper extends AbstractFieldMapper<Uid> implements InternalMapper, RootMapper {

    public static final String NAME = "_uid".intern();

    public static final String CONTENT_TYPE = "_uid";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = UidFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);
        public static final FieldType NESTED_FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);
        static final FieldType STORED_ONLY_FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);
        static {
            FIELD_TYPE.setIndexed(true);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
            FIELD_TYPE.setDocValueType(DocValuesType.NUMERIC);
            FIELD_TYPE.freeze();

            NESTED_FIELD_TYPE.setIndexed(true);
            NESTED_FIELD_TYPE.setTokenized(false);
            NESTED_FIELD_TYPE.setStored(false);
            NESTED_FIELD_TYPE.setOmitNorms(true);
            NESTED_FIELD_TYPE.freeze();
            
            STORED_ONLY_FIELD_TYPE.setStored(true);
            STORED_ONLY_FIELD_TYPE.setIndexed(false);
            STORED_ONLY_FIELD_TYPE.setTokenized(false);
            STORED_ONLY_FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends Mapper.Builder<Builder, UidFieldMapper> {

        protected String indexName;
        protected PostingsFormatProvider postingsFormat;
        protected DocValuesFormatProvider docValues;

        public Builder() {
            super(Defaults.NAME);
            this.indexName = name;
        }

        @Override
        public UidFieldMapper build(BuilderContext context) {
            return new UidFieldMapper(name, indexName, postingsFormat, docValues);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = uid();
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("postings_format")) {
                    String postingFormatName = fieldNode.toString();
                    builder.postingsFormat = parserContext.postingFormatService().get(postingFormatName);
                } else if (fieldName.equals("docvalues_format")) {
                    String docValuesFormatName = fieldNode.toString();
                    builder.docValues = parserContext.docValuesFormatService().get(docValuesFormatName);
                }
            }
            return builder;
        }
    }


    public UidFieldMapper() {
        this(Defaults.NAME);
    }

    protected UidFieldMapper(String name) {
        this(name, name, null, null);
    }

    protected UidFieldMapper(String name, String indexName, PostingsFormatProvider postingsFormat, DocValuesFormatProvider docValues) {
        super(new Names(name, indexName, indexName, name), Defaults.BOOST, new FieldType(Defaults.FIELD_TYPE),
                Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER, postingsFormat, docValues, null, null);
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
    protected String defaultPostingFormat() {
        return "bloom_default";
    }
    
    protected String defaultDocValuesFormat() {
        return "Disk";
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        // if we have the id provided, fill it, and parse now
        if (context.sourceToParse().id() != null) {
            context.id(context.sourceToParse().id());
            innerParse(context);
        }
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        if (context.id() == null && !context.sourceToParse().flyweight()) {
            throw new MapperParsingException("No id found while parsing the content source");
        }
        // if we did not have the id as part of the sourceToParse, then we need to parse it here
        // it would have been filled in the _id parse phase
        if (context.sourceToParse().id() == null) {
            innerParse(context);
            // since we did not have the uid in the pre phase, we did not add it automatically to the nested docs
            // as they were created we need to make sure we add it to all the nested docs...
            if (context.docs().size() > 1) {
                UidField uidField = (UidField) context.rootDoc().getField(UidFieldMapper.NAME);
                assert uidField != null;
                // we need to go over the docs and add it...
                for (int i = 1; i < context.docs().size(); i++) {
                    // we don't need to add it as a full uid field in nested docs, since we don't need versioning
                    context.docs().get(i).add(new Field(UidFieldMapper.NAME, uidField.uid(), Defaults.NESTED_FIELD_TYPE));
                }
            }
        }
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // nothing to do here, we either do it in post parse, or in pre parse.
    }
    
    
    final void innerParse(ParseContext context) throws IOException {
        try {
            Field field = parseCreateField(context);
            if (!customBoost()) {
                field.setBoost(boost);
            }
            if (context.listener().beforeFieldAdded(this, field, context)) {
                context.doc().add(new Field(field.name(), field.stringValue(), Defaults.STORED_ONLY_FIELD_TYPE));
                context.doc().add(field);
            }
        } catch (Exception e) {
            throw new MapperParsingException("failed to parse [" + names.fullName() + "]", e);
        }
    }

    @Override
    public void validate(ParseContext context) throws MapperParsingException {
    }

    @Override
    public boolean includeInObject() {
        return false;
    }

    @Override
    protected Field parseCreateField(ParseContext context) throws IOException {
        // so, caching uid stream and field is fine
        // since we don't do any mapping parsing without immediate indexing
        // and, when percolating, we don't index the uid
        UidField field = new UidField(Uid.createUid(context.stringBuilder(), context.type(), context.id()));
        // version get updated by the engine
        context.uid(field);
        return field;
    }

    @Override
    public Uid value(Object value) {
        if (value == null) {
            return null;
        }
        return Uid.createUid(value.toString());
    }

    public Term term(String type, String id) {
        return term(Uid.createUid(type, id));
    }

    public Term term(String uid) {
        return names().createIndexNameTerm(uid);
    }

    @Override
    public void close() {
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // if defaults, don't output
        if ((postingsFormat == null || postingsFormat.name().equals(defaultPostingFormat()))) {
            return builder;
        }

        builder.startObject(CONTENT_TYPE);

        if (postingsFormat != null) {
            if (!postingsFormat.name().equals(defaultPostingFormat())) {
                builder.field("postings_format", postingsFormat.name());
            }
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        AbstractFieldMapper fieldMergeWith = (AbstractFieldMapper) mergeWith;
        // do nothing here, no merging, but also no exception
        if (!mergeContext.mergeFlags().simulate()) {
            // apply changeable values
            if (fieldMergeWith.postingsFormatProvider() != null) {
                this.postingsFormat = fieldMergeWith.postingsFormatProvider();
            }
        }
    }
}
