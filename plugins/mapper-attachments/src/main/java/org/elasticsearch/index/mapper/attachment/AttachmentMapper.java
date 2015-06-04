/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.attachment;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.util.Constants;
import org.apache.tika.Tika;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.elasticsearch.Version;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.*;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parsePathType;
import static org.elasticsearch.plugin.mapper.attachments.tika.TikaInstance.tika;

/**
 * <pre>
 *      "field1" : "..."
 * </pre>
 * <p>Or:
 * <pre>
 * {
 *      "file1" : {
 *          "_content_type" : "application/pdf",
 *          "_content_length" : "500000000",
 *          "_name" : "..../something.pdf",
 *          "_content" : ""
 *      }
 * }
 * </pre>
 * <p/>
 * _content_length = Specify the maximum amount of characters to extract from the attachment. If not specified, then the default for
 * tika is 100,000 characters. Caution is required when setting large values as this can cause memory issues.
 */
public class AttachmentMapper extends AbstractFieldMapper {

    private static ESLogger logger = ESLoggerFactory.getLogger("mapper.attachment");

    public static final String CONTENT_TYPE = "attachment";

    public static class Defaults {
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
    }

    public static class FieldNames {
        public static final String CONTENT = "content";
        public static final String TITLE = "title";
        public static final String NAME = "name";
        public static final String AUTHOR = "author";
        public static final String KEYWORDS = "keywords";
        public static final String DATE = "date";
        public static final String CONTENT_TYPE = "content_type";
        public static final String CONTENT_LENGTH = "content_length";
        public static final String LANGUAGE = "language";
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, AttachmentMapper> {

        private ContentPath.Type pathType = Defaults.PATH_TYPE;

        private Boolean ignoreErrors = null;

        private Integer defaultIndexedChars = null;

        private Boolean langDetect = null;

        private Mapper.Builder contentBuilder;

        private Mapper.Builder titleBuilder = stringField(FieldNames.TITLE);

        private Mapper.Builder nameBuilder = stringField(FieldNames.NAME);

        private Mapper.Builder authorBuilder = stringField(FieldNames.AUTHOR);

        private Mapper.Builder keywordsBuilder = stringField(FieldNames.KEYWORDS);

        private Mapper.Builder dateBuilder = dateField(FieldNames.DATE);

        private Mapper.Builder contentTypeBuilder = stringField(FieldNames.CONTENT_TYPE);

        private Mapper.Builder contentLengthBuilder = integerField(FieldNames.CONTENT_LENGTH);

        private Mapper.Builder languageBuilder = stringField(FieldNames.LANGUAGE);

        public Builder(String name) {
            super(name, new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE));
            this.builder = this;
            this.contentBuilder = stringField(FieldNames.CONTENT);
        }

        public Builder pathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        public Builder content(Mapper.Builder content) {
            this.contentBuilder = content;
            return this;
        }

        public Builder date(Mapper.Builder date) {
            this.dateBuilder = date;
            return this;
        }

        public Builder author(Mapper.Builder author) {
            this.authorBuilder = author;
            return this;
        }

        public Builder title(Mapper.Builder title) {
            this.titleBuilder = title;
            return this;
        }

        public Builder name(Mapper.Builder name) {
            this.nameBuilder = name;
            return this;
        }

        public Builder keywords(Mapper.Builder keywords) {
            this.keywordsBuilder = keywords;
            return this;
        }

        public Builder contentType(Mapper.Builder contentType) {
            this.contentTypeBuilder = contentType;
            return this;
        }

        public Builder contentLength(Mapper.Builder contentType) {
            this.contentLengthBuilder = contentType;
            return this;
        }

        public Builder language(Mapper.Builder language) {
            this.languageBuilder = language;
            return this;
        }

        @Override
        public AttachmentMapper build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            FieldMapper contentMapper;
            if (context.indexCreatedVersion().before(Version.V_2_0_0)) {
                // old behavior, we need the content to be indexed under the attachment field name
                if (contentBuilder instanceof AbstractFieldMapper.Builder == false) {
                    throw new IllegalStateException("content field for attachment must be a field mapper");
                }
                ((AbstractFieldMapper.Builder)contentBuilder).indexName(name);
                contentBuilder.name = name + "." + FieldNames.CONTENT;
                contentMapper = (FieldMapper) contentBuilder.build(context);
                context.path().add(name);
            } else {
                context.path().add(name);
                contentMapper = (FieldMapper) contentBuilder.build(context);
            }

            FieldMapper dateMapper = (FieldMapper) dateBuilder.build(context);
            FieldMapper authorMapper = (FieldMapper) authorBuilder.build(context);
            FieldMapper titleMapper = (FieldMapper) titleBuilder.build(context);
            FieldMapper nameMapper = (FieldMapper) nameBuilder.build(context);
            FieldMapper keywordsMapper = (FieldMapper) keywordsBuilder.build(context);
            FieldMapper contentTypeMapper = (FieldMapper) contentTypeBuilder.build(context);
            FieldMapper contentLength = (FieldMapper) contentLengthBuilder.build(context);
            FieldMapper language = (FieldMapper) languageBuilder.build(context);
            context.path().remove();

            context.path().pathType(origPathType);

            if (defaultIndexedChars == null && context.indexSettings() != null) {
                defaultIndexedChars = context.indexSettings().getAsInt("index.mapping.attachment.indexed_chars", 100000);
            }
            if (defaultIndexedChars == null) {
                defaultIndexedChars = 100000;
            }

            if (ignoreErrors == null && context.indexSettings() != null) {
                ignoreErrors = context.indexSettings().getAsBoolean("index.mapping.attachment.ignore_errors", Boolean.TRUE);
            }
            if (ignoreErrors == null) {
                ignoreErrors = Boolean.TRUE;
            }

            if (langDetect == null && context.indexSettings() != null) {
                langDetect = context.indexSettings().getAsBoolean("index.mapping.attachment.detect_language", Boolean.FALSE);
            }
            if (langDetect == null) {
                langDetect = Boolean.FALSE;
            }

            return new AttachmentMapper(buildNames(context), pathType, defaultIndexedChars, ignoreErrors, langDetect, contentMapper,
                    dateMapper, titleMapper, nameMapper, authorMapper, keywordsMapper, contentTypeMapper, contentLength,
                    language, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    /**
     * <pre>
     *  field1 : { type : "attachment" }
     * </pre>
     * Or:
     * <pre>
     *  field1 : {
     *      type : "attachment",
     *      fields : {
     *          content : {type : "binary"},
     *          title : {store : "yes"},
     *          date : {store : "yes"},
     *          name : {store : "yes"},
     *          author : {store : "yes"},
     *          keywords : {store : "yes"},
     *          content_type : {store : "yes"},
     *          content_length : {store : "yes"}
     *      }
     * }
     * </pre>
     */
    public static class TypeParser implements Mapper.TypeParser {

        private Mapper.Builder<?, ?> findMapperBuilder(Map<String, Object> propNode, String propName, ParserContext parserContext) {
            String type;
            Object typeNode = propNode.get("type");
            if (typeNode != null) {
                type = typeNode.toString();
            } else {
                type = "string";
            }
            Mapper.TypeParser typeParser = parserContext.typeParser(type);
            Mapper.Builder<?, ?> mapperBuilder = typeParser.parse(propName, (Map<String, Object>) propNode, parserContext);

            return mapperBuilder;
        }

        @SuppressWarnings({"unchecked"})
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            AttachmentMapper.Builder builder = new AttachmentMapper.Builder(name);

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path") && parserContext.indexVersionCreated().before(Version.V_2_0_0)) {
                    builder.pathType(parsePathType(name, fieldNode.toString()));
                    iterator.remove();
                } else if (fieldName.equals("fields")) {
                    Map<String, Object> fieldsNode = (Map<String, Object>) fieldNode;
                    for (Iterator<Map.Entry<String, Object>> fieldsIterator = fieldsNode.entrySet().iterator(); fieldsIterator.hasNext();) {
                        Map.Entry<String, Object> entry1 = fieldsIterator.next();
                        String propName = entry1.getKey();
                        Map<String, Object> propNode = (Map<String, Object>) entry1.getValue();

                        Mapper.Builder<?, ?> mapperBuilder = findMapperBuilder(propNode, propName, parserContext);
                        if (parseMultiField((AbstractFieldMapper.Builder) mapperBuilder, fieldName, parserContext, propName, propNode)) {
                            fieldsIterator.remove();
                        } else if (propName.equals(name) && parserContext.indexVersionCreated().before(Version.V_2_0_0)) {
                            builder.content(mapperBuilder);
                            fieldsIterator.remove();
                        } else {
                            switch (propName) {
                                case FieldNames.CONTENT:
                                    builder.content(mapperBuilder);
                                    fieldsIterator.remove();
                                    break;
                                case FieldNames.DATE:
                                    builder.date(mapperBuilder);
                                fieldsIterator.remove();
                                    break;
                                case FieldNames.AUTHOR:
                                    builder.author(mapperBuilder);
                                fieldsIterator.remove();
                                    break;
                                case FieldNames.CONTENT_LENGTH:
                                    builder.contentLength(mapperBuilder);
                                fieldsIterator.remove();
                                    break;
                                case FieldNames.CONTENT_TYPE:
                                    builder.contentType(mapperBuilder);
                                fieldsIterator.remove();
                                    break;
                                case FieldNames.KEYWORDS:
                                    builder.keywords(mapperBuilder);
                                fieldsIterator.remove();
                                    break;
                                case FieldNames.LANGUAGE:
                                    builder.language(mapperBuilder);
                                fieldsIterator.remove();
                                    break;
                                case FieldNames.TITLE:
                                    builder.title(mapperBuilder);
                                fieldsIterator.remove();
                                    break;
                                case FieldNames.NAME:
                                    builder.name(mapperBuilder);
                                fieldsIterator.remove();
                                    break;
                            }
                        }
                    }
                    DocumentMapperParser.checkNoRemainingFields(fieldName, fieldsNode, parserContext.indexVersionCreated());
                    iterator.remove();
                }
            }

            return builder;
        }
    }

    private final ContentPath.Type pathType;

    private final int defaultIndexedChars;

    private final boolean ignoreErrors;

    private final boolean defaultLangDetect;

    private final FieldMapper contentMapper;

    private final FieldMapper dateMapper;

    private final FieldMapper authorMapper;

    private final FieldMapper titleMapper;

    private final FieldMapper nameMapper;

    private final FieldMapper keywordsMapper;

    private final FieldMapper contentTypeMapper;

    private final FieldMapper contentLengthMapper;

    private final FieldMapper languageMapper;

    public AttachmentMapper(Names names, ContentPath.Type pathType, int defaultIndexedChars, Boolean ignoreErrors,
                            Boolean defaultLangDetect, FieldMapper contentMapper,
                            FieldMapper dateMapper, FieldMapper titleMapper, FieldMapper nameMapper, FieldMapper authorMapper,
                            FieldMapper keywordsMapper, FieldMapper contentTypeMapper, FieldMapper contentLengthMapper,
                            FieldMapper languageMapper, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(names, 1.0f, AbstractFieldMapper.Defaults.FIELD_TYPE, false, null, null, null, null, null,
                indexSettings, multiFields, copyTo);
        this.pathType = pathType;
        this.defaultIndexedChars = defaultIndexedChars;
        this.ignoreErrors = ignoreErrors;
        this.defaultLangDetect = defaultLangDetect;
        this.contentMapper = contentMapper;
        this.dateMapper = dateMapper;
        this.titleMapper = titleMapper;
        this.nameMapper = nameMapper;
        this.authorMapper = authorMapper;
        this.keywordsMapper = keywordsMapper;
        this.contentTypeMapper = contentTypeMapper;
        this.contentLengthMapper = contentLengthMapper;
        this.languageMapper = languageMapper;
    }

    @Override
    public Object value(Object value) {
        return null;
    }

    @Override
    public FieldType defaultFieldType() {
        return AbstractFieldMapper.Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return null;
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        byte[] content = null;
        String contentType = null;
        int indexedChars = defaultIndexedChars;
        boolean langDetect = defaultLangDetect;
        String name = null;
        String language = null;

        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            content = parser.binaryValue();
        } else {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if ("_content".equals(currentFieldName)) {
                        content = parser.binaryValue();
                    } else if ("_content_type".equals(currentFieldName)) {
                        contentType = parser.text();
                    } else if ("_name".equals(currentFieldName)) {
                        name = parser.text();
                    } else if ("_language".equals(currentFieldName)) {
                        language = parser.text();
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if ("_indexed_chars".equals(currentFieldName) || "_indexedChars".equals(currentFieldName)) {
                        indexedChars = parser.intValue();
                    }
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if ("_detect_language".equals(currentFieldName) || "_detectLanguage".equals(currentFieldName)) {
                        langDetect = parser.booleanValue();
                    }
                }
            }
        }

        // Throw clean exception when no content is provided Fix #23
        if (content == null) {
            throw new MapperParsingException("No content is provided.");
        }

        Metadata metadata = new Metadata();
        if (contentType != null) {
            metadata.add(Metadata.CONTENT_TYPE, contentType);
        }
        if (name != null) {
            metadata.add(Metadata.RESOURCE_NAME_KEY, name);
        }

        String parsedContent;
        try {
            Tika tika = tika();
            if (tika == null) {
                if (!ignoreErrors) {
                    throw new MapperParsingException("Tika can not be initialized with the current Locale [" +
                            Locale.getDefault().getLanguage() + "] on the current JVM [" +
                            Constants.JAVA_VERSION + "]");
                } else {
                    logger.warn("Tika can not be initialized with the current Locale [{}] on the current JVM [{}]",
                            Locale.getDefault().getLanguage(), Constants.JAVA_VERSION);
                    return null;
                }
            }
            // Set the maximum length of strings returned by the parseToString method, -1 sets no limit
            parsedContent = tika.parseToString(StreamInput.wrap(content), metadata, indexedChars);
        } catch (Throwable e) {
            // It could happen that Tika adds a System property `sun.font.fontmanager` which should not happen
            // TODO Remove when this will be fixed in Tika. See https://issues.apache.org/jira/browse/TIKA-1548
            System.clearProperty("sun.font.fontmanager");

            // #18: we could ignore errors when Tika does not parse data
            if (!ignoreErrors) {
                logger.trace("exception caught", e);
                throw new MapperParsingException("Failed to extract [" + indexedChars + "] characters of text for [" + name + "] : "
                        + e.getMessage(), e);
            } else {
                logger.debug("Failed to extract [{}] characters of text for [{}]: [{}]", indexedChars, name, e.getMessage());
                logger.trace("exception caught", e);
            }
            return null;
        }

        context = context.createExternalValueContext(parsedContent);
        contentMapper.parse(context);

        if (langDetect) {
            try {
                if (language != null) {
                    metadata.add(Metadata.CONTENT_LANGUAGE, language);
                } else {
                    LanguageIdentifier identifier = new LanguageIdentifier(parsedContent);
                    language = identifier.getLanguage();
                }
                context = context.createExternalValueContext(language);
                languageMapper.parse(context);
            } catch(Throwable t) {
                logger.debug("Cannot detect language: [{}]", t.getMessage());
            }
        }

        if (name != null) {
            try {
                context = context.createExternalValueContext(name);
                nameMapper.parse(context);
            } catch(MapperParsingException e){
                if (!ignoreErrors) throw e;
                if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing name: [{}]",
                        e.getMessage());
            }
        }

        if (metadata.get(Metadata.DATE) != null) {
            try {
                context = context.createExternalValueContext(metadata.get(Metadata.DATE));
                dateMapper.parse(context);
            } catch(MapperParsingException e){
                if (!ignoreErrors) throw e;
                if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing date: [{}]: [{}]",
                        e.getMessage(), context.externalValue());
            }
        }

        if (metadata.get(Metadata.TITLE) != null) {
            try {
                context = context.createExternalValueContext(metadata.get(Metadata.TITLE));
                titleMapper.parse(context);
            } catch(MapperParsingException e){
                if (!ignoreErrors) throw e;
                if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing title: [{}]: [{}]",
                        e.getMessage(), context.externalValue());
            }
        }

        if (metadata.get(Metadata.AUTHOR) != null) {
            try {
                context = context.createExternalValueContext(metadata.get(Metadata.AUTHOR));
                authorMapper.parse(context);
            } catch(MapperParsingException e){
                if (!ignoreErrors) throw e;
                if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing author: [{}]: [{}]",
                        e.getMessage(), context.externalValue());
            }
        }

        if (metadata.get(Metadata.KEYWORDS) != null) {
            try {
                context = context.createExternalValueContext(metadata.get(Metadata.KEYWORDS));
                keywordsMapper.parse(context);
            } catch(MapperParsingException e){
                if (!ignoreErrors) throw e;
                if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing keywords: [{}]: [{}]",
                        e.getMessage(), context.externalValue());
            }
        }

        if (contentType == null) {
            contentType = metadata.get(Metadata.CONTENT_TYPE);
        }
        if (contentType != null) {
            try {
                context = context.createExternalValueContext(contentType);
                contentTypeMapper.parse(context);
            } catch(MapperParsingException e){
                if (!ignoreErrors) throw e;
                if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing content_type: [{}]: [{}]", e.getMessage(), context.externalValue());
            }
        }

        int length = content.length;
        // If we have CONTENT_LENGTH from Tika we use it
        if (metadata.get(Metadata.CONTENT_LENGTH) != null) {
            length = Integer.parseInt(metadata.get(Metadata.CONTENT_LENGTH));
        }

        try {
            context = context.createExternalValueContext(length);
            contentLengthMapper.parse(context);
        } catch(MapperParsingException e){
            if (!ignoreErrors) throw e;
            if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing content_length: [{}]: [{}]", e.getMessage(), context.externalValue());
        }

//        multiFields.parse(this, context);

        return null;
    }

    @Override
    protected void parseCreateField(ParseContext parseContext, List<Field> fields) throws IOException {

    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        // ignore this for now
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<FieldMapper> extras = Lists.newArrayList(
            contentMapper,
            dateMapper,
            titleMapper,
            nameMapper,
            authorMapper,
            keywordsMapper,
            contentTypeMapper,
            contentLengthMapper,
            languageMapper);
        return Iterators.concat(super.iterator(), extras.iterator());
    }

    @Override
    public void close() {
        contentMapper.close();
        dateMapper.close();
        titleMapper.close();
        nameMapper.close();
        authorMapper.close();
        keywordsMapper.close();
        contentTypeMapper.close();
        contentLengthMapper.close();
        languageMapper.close();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name());
        builder.field("type", CONTENT_TYPE);
        if (indexCreatedBefore2x) {
            builder.field("path", pathType.name().toLowerCase(Locale.ROOT));
        }

        builder.startObject("fields");
        contentMapper.toXContent(builder, params);
        authorMapper.toXContent(builder, params);
        titleMapper.toXContent(builder, params);
        nameMapper.toXContent(builder, params);
        dateMapper.toXContent(builder, params);
        keywordsMapper.toXContent(builder, params);
        contentTypeMapper.toXContent(builder, params);
        contentLengthMapper.toXContent(builder, params);
        languageMapper.toXContent(builder, params);
        multiFields.toXContent(builder, params);
        builder.endObject();

        multiFields.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
