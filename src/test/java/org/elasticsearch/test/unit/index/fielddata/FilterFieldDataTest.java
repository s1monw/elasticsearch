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
package org.elasticsearch.test.unit.index.fielddata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.AtomicFieldData.WithOrdinals;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues.Strings;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.mapper.FieldMapper;
import org.testng.annotations.Test;

public class FilterFieldDataTest extends AbstractFieldDataTests {

    @Override
    protected FieldDataType getFieldDataType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Test
    public void testFilterByFrequency() throws Exception {

        int hundred  = 0;
        int ten  = 0;
        int five  = 0;
        for (int i = 0; i < 1000; i++) {
            Document d = new Document();
            d.add(new StringField("id", "" + i, Field.Store.NO));
            if (i % 100 == 0) {
                hundred++;
                d.add(new StringField("high_freq", "100", Field.Store.NO));
                d.add(new StringField("low_freq", "100", Field.Store.NO));
                d.add(new StringField("med_freq", "100", Field.Store.NO));
            }
            if (i % 10 == 0) {
                ten++;
                d.add(new StringField("high_freq", "10", Field.Store.NO));
                d.add(new StringField("med_freq", "10", Field.Store.NO));
            }
            if (i % 5 == 0) {
                five++;
                d.add(new StringField("high_freq", "5", Field.Store.NO));

            }
            writer.addDocument(d);
        }
        System.out.println(hundred + " " + ten + " " +five);
        writer.forceMerge(1);
        AtomicReaderContext context = refreshReader();
        String[] formats = new String[] { "fst", "paged_bytes", "concrete_bytes" };
        for (String format : formats) {

            {
                ifdService.clear();
                FieldDataType fieldDataType = new FieldDataType("string", ImmutableSettings.builder().put("format", format)
                        .put("filter.frequency.min_field_doc_count", 100).put("filter.frequency.min", 0).put("filter.frequency.max", 100));
                IndexFieldData fieldData = ifdService.getForField(new FieldMapper.Names("high_freq"), fieldDataType);
                AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> loadDirect = (WithOrdinals<Strings>) fieldData.loadDirect(context);
                BytesValues.WithOrdinals bytesValues = loadDirect.getBytesValues();
                Docs ordinals = bytesValues.ordinals();
                assertThat(2, equalTo(ordinals.getNumOrds()));
                assertThat(1000, equalTo(ordinals.getNumDocs()));
                assertThat(bytesValues.getValueByOrd(1).utf8ToString(), equalTo("10"));
                assertThat(bytesValues.getValueByOrd(2).utf8ToString(), equalTo("100"));
            }
            {
                ifdService.clear();
                FieldDataType fieldDataType = new FieldDataType("string", ImmutableSettings.builder().put("format", format)
                        .put("filter.frequency.min_field_doc_count", 100).put("filter.frequency.min", 101).put("filter.frequency.max", 201));
                IndexFieldData fieldData = ifdService.getForField(new FieldMapper.Names("high_freq"), fieldDataType);
                AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> loadDirect = (WithOrdinals<Strings>) fieldData.loadDirect(context);
                BytesValues.WithOrdinals bytesValues = loadDirect.getBytesValues();
                Docs ordinals = bytesValues.ordinals();
                assertThat(1, equalTo(ordinals.getNumOrds()));
                assertThat(1000, equalTo(ordinals.getNumDocs()));
                assertThat(bytesValues.getValueByOrd(1).utf8ToString(), equalTo("5"));
            }
            
            {
                ifdService.clear();
                FieldDataType fieldDataType = new FieldDataType("string", ImmutableSettings.builder().put("format", format)
                        .put("filter.frequency.min_field_doc_count", 101).put("filter.frequency.min", 101).put("filter.frequency.max", 201));
                IndexFieldData fieldData = ifdService.getForField(new FieldMapper.Names("med_freq"), fieldDataType);
                AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> loadDirect = (WithOrdinals<Strings>) fieldData.loadDirect(context);
                BytesValues.WithOrdinals bytesValues = loadDirect.getBytesValues();
                Docs ordinals = bytesValues.ordinals();
                assertThat(2, equalTo(ordinals.getNumOrds()));
                assertThat(1000, equalTo(ordinals.getNumDocs()));
                assertThat(bytesValues.getValueByOrd(1).utf8ToString(), equalTo("10"));
                assertThat(bytesValues.getValueByOrd(2).utf8ToString(), equalTo("100"));
            }
        }

    }
    
    @Test
    public void testFilterByRegExp() throws Exception {

        int hundred  = 0;
        int ten  = 0;
        int five  = 0;
        for (int i = 0; i < 1000; i++) {
            Document d = new Document();
            d.add(new StringField("id", "" + i, Field.Store.NO));
            if (i % 100 == 0) {
                hundred++;
                d.add(new StringField("high_freq", "100", Field.Store.NO));
            }
            if (i % 10 == 0) {
                ten++;
                d.add(new StringField("high_freq", "10", Field.Store.NO));
            }
            if (i % 5 == 0) {
                five++;
                d.add(new StringField("high_freq", "5", Field.Store.NO));

            }
            writer.addDocument(d);
        }
        System.out.println(hundred + " " + ten + " " +five);
        writer.forceMerge(1);
        AtomicReaderContext context = refreshReader();
        String[] formats = new String[] { "fst", "paged_bytes", "concrete_bytes" };
        for (String format : formats) {
            {
                ifdService.clear();
                FieldDataType fieldDataType = new FieldDataType("string", ImmutableSettings.builder().put("format", format)
                        .put("filter.regex.pattern", "\\d"));
                IndexFieldData fieldData = ifdService.getForField(new FieldMapper.Names("high_freq"), fieldDataType);
                AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> loadDirect = (WithOrdinals<Strings>) fieldData.loadDirect(context);
                BytesValues.WithOrdinals bytesValues = loadDirect.getBytesValues();
                Docs ordinals = bytesValues.ordinals();
                assertThat(1, equalTo(ordinals.getNumOrds()));
                assertThat(1000, equalTo(ordinals.getNumDocs()));
                assertThat(bytesValues.getValueByOrd(1).utf8ToString(), equalTo("5"));
            }
            {
                ifdService.clear();
                FieldDataType fieldDataType = new FieldDataType("string", ImmutableSettings.builder().put("format", format)
                        .put("filter.regex.pattern", "\\d{1,2}"));
                IndexFieldData fieldData = ifdService.getForField(new FieldMapper.Names("high_freq"), fieldDataType);
                AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> loadDirect = (WithOrdinals<Strings>) fieldData.loadDirect(context);
                BytesValues.WithOrdinals bytesValues = loadDirect.getBytesValues();
                Docs ordinals = bytesValues.ordinals();
                assertThat(2, equalTo(ordinals.getNumOrds()));
                assertThat(1000, equalTo(ordinals.getNumDocs()));
                assertThat(bytesValues.getValueByOrd(1).utf8ToString(), equalTo("10"));
                assertThat(bytesValues.getValueByOrd(2).utf8ToString(), equalTo("5"));
            }
        }

    }


}
