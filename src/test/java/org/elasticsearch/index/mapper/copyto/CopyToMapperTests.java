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

package org.elasticsearch.index.mapper.copyto;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.DocumentMapper.MergeFlags.mergeFlags;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class CopyToMapperTests extends ElasticsearchTestCase {

    @SuppressWarnings("unchecked")
    @Test
    public void testCopyToFieldsParsing() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("copy_test")
                .field("type", "string")
                .array("copy_to", "another_field", "cyclic_test")
                .endObject()

                .startObject("another_field")
                .field("type", "string")
                .endObject()

                .startObject("cyclic_test")
                .field("type", "string")
                .array("copy_to", "copy_test")
                .endObject()

                .startObject("int_to_str_test")
                .field("type", "integer")
                .array("copy_to",  "another_field", "new_field")
                .endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        FieldMapper fieldMapper = docMapper.mappers().name("copy_test").mapper();
        assertThat(fieldMapper, instanceOf(StringFieldMapper.class));

        // Check json serialization
        StringFieldMapper stringFieldMapper = (StringFieldMapper) fieldMapper;
        XContentBuilder builder = jsonBuilder().startObject();
        stringFieldMapper.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        builder.close();
        Map<String, Object> serializedMap = JsonXContent.jsonXContent.createParser(builder.bytes()).mapAndClose();
        Map<String, Object> copyTestMap = (Map<String, Object>) serializedMap.get("copy_test");
        assertThat(copyTestMap.get("type").toString(), is("string"));
        List<String> copyToList = (List<String>) copyTestMap.get("copy_to");
        assertThat(copyToList.size(), equalTo(2));
        assertThat(copyToList.get(0).toString(), equalTo("another_field"));
        assertThat(copyToList.get(1).toString(), equalTo("cyclic_test"));

        // Check data parsing
        BytesReference json = jsonBuilder().startObject()
                .field("copy_test", "foo")
                .field("cyclic_test", "bar")
                .field("int_to_str_test", 42)
                .endObject().bytes();

        ParseContext.Document doc = docMapper.parse("type1", "1", json, org.elasticsearch.Version.CURRENT).rootDoc();
        assertThat(doc.getFields("copy_test").length, equalTo(2));
        assertThat(doc.getFields("copy_test")[0].stringValue(), equalTo("foo"));
        assertThat(doc.getFields("copy_test")[1].stringValue(), equalTo("bar"));

        assertThat(doc.getFields("another_field").length, equalTo(2));
        assertThat(doc.getFields("another_field")[0].stringValue(), equalTo("foo"));
        assertThat(doc.getFields("another_field")[1].stringValue(), equalTo("42"));

        assertThat(doc.getFields("cyclic_test").length, equalTo(2));
        assertThat(doc.getFields("cyclic_test")[0].stringValue(), equalTo("foo"));
        assertThat(doc.getFields("cyclic_test")[1].stringValue(), equalTo("bar"));

        assertThat(doc.getFields("int_to_str_test").length, equalTo(1));
        assertThat(doc.getFields("int_to_str_test")[0].numericValue().intValue(), equalTo(42));

        assertThat(doc.getFields("new_field").length, equalTo(1));
        assertThat(doc.getFields("new_field")[0].numericValue().intValue(), equalTo(42));

        fieldMapper = docMapper.mappers().name("new_field").mapper();
        assertThat(fieldMapper, instanceOf(LongFieldMapper.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCopyToFieldsInnerObjectParsing() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")

                .startObject("copy_test")
                .field("type", "string")
                .field("copy_to", "very.inner.field")
                .endObject()

                .startObject("very")
                .field("type", "object")
                .startObject("properties")
                .startObject("inner")
                .field("type", "object")
                .endObject()
                .endObject()
                .endObject()

                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);

        BytesReference json = jsonBuilder().startObject()
                .field("copy_test", "foo")
                .startObject("foo").startObject("bar").field("baz", "zoo").endObject().endObject()
                .endObject().bytes();

        ParseContext.Document doc = docMapper.parse("type1", "1", json, org.elasticsearch.Version.CURRENT).rootDoc();
        assertThat(doc.getFields("copy_test").length, equalTo(1));
        assertThat(doc.getFields("copy_test")[0].stringValue(), equalTo("foo"));

        assertThat(doc.getFields("very.inner.field").length, equalTo(1));
        assertThat(doc.getFields("very.inner.field")[0].stringValue(), equalTo("foo"));

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCopyToFieldsNonExistingInnerObjectParsing() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")

                .startObject("copy_test")
                .field("type", "string")
                .field("copy_to", "very.inner.field")
                .endObject()

                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);

        BytesReference json = jsonBuilder().startObject()
                .field("copy_test", "foo")
                .endObject().bytes();

        try {
            docMapper.parse("type1", "1", json, org.elasticsearch.Version.CURRENT).rootDoc();
            fail();
        } catch (MapperParsingException ex) {
            assertThat(ex.getMessage(), startsWith("attempt to copy value to non-existing object"));
        }
    }

    @Test
    public void testCopyToFieldMerge() throws Exception {

        String mappingBefore = jsonBuilder().startObject().startObject("type1").startObject("properties")

                .startObject("copy_test")
                .field("type", "string")
                .array("copy_to", "foo", "bar")
                .endObject()

                .endObject().endObject().endObject().string();

        String mappingAfter = jsonBuilder().startObject().startObject("type1").startObject("properties")

                .startObject("copy_test")
                .field("type", "string")
                .array("copy_to", "baz", "bar")
                .endObject()

                .endObject().endObject().endObject().string();

        DocumentMapper docMapperBefore = MapperTestUtils.newParser().parse(mappingBefore);

        ImmutableList<String> fields = docMapperBefore.mappers().name("copy_test").mapper().copyTo().copyToFields();

        assertThat(fields.size(), equalTo(2));
        assertThat(fields.get(0), equalTo("foo"));
        assertThat(fields.get(1), equalTo("bar"));


        DocumentMapper docMapperAfter = MapperTestUtils.newParser().parse(mappingAfter);

        DocumentMapper.MergeResult mergeResult = docMapperBefore.merge(docMapperAfter, mergeFlags().simulate(true));

        assertThat(Arrays.toString(mergeResult.conflicts()), mergeResult.hasConflicts(), equalTo(false));

        docMapperBefore.merge(docMapperAfter, mergeFlags().simulate(false));

        fields = docMapperBefore.mappers().name("copy_test").mapper().copyTo().copyToFields();

        assertThat(fields.size(), equalTo(2));
        assertThat(fields.get(0), equalTo("baz"));
        assertThat(fields.get(1), equalTo("bar"));
    }

}
