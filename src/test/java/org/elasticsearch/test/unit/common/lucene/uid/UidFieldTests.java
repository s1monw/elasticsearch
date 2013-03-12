/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.unit.common.lucene.uid;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.hamcrest.MatcherAssert;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class UidFieldTests {

    @Test
    public void testUidField() throws Exception {
        IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        DirectoryReader directoryReader = DirectoryReader.open(writer, true);
        AtomicReader atomicReader = SlowCompositeReaderWrapper.wrap(directoryReader);
        MatcherAssert.assertThat(UidField.loadVersion(atomicReader.getContext(), new Term("_uid", "1")), equalTo(-1l));
        FieldType legacyFieldType = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);
        legacyFieldType.setIndexed(true);
        legacyFieldType.setTokenized(false);
        legacyFieldType.setStored(true);
        legacyFieldType.setOmitNorms(true);
        legacyFieldType.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        legacyFieldType.freeze();
        Document doc = new Document();
        doc.add(new Field("_uid", "1", legacyFieldType));
        writer.addDocument(doc);
        directoryReader = DirectoryReader.openIfChanged(directoryReader);
        atomicReader = SlowCompositeReaderWrapper.wrap(directoryReader);
        assertThat(UidField.loadVersion(atomicReader.getContext(), new Term("_uid", "1")), equalTo(-2l));
        assertThat(UidField.loadDocIdAndVersion(atomicReader.getContext(), new Term("_uid", "1")).version, equalTo(-2l));

        doc = new Document();
        doc.add(new UidField("_uid", "1", 1));
        writer.updateDocument(new Term("_uid", "1"), doc);
        directoryReader = DirectoryReader.openIfChanged(directoryReader);
        atomicReader = SlowCompositeReaderWrapper.wrap(directoryReader);
        assertThat(UidField.loadVersion(atomicReader.getContext(), new Term("_uid", "1")), equalTo(1l));
        assertThat(UidField.loadDocIdAndVersion(atomicReader.getContext(), new Term("_uid", "1")).version, equalTo(1l));

        doc = new Document();
        UidField uid = new UidField("_uid", "1", 2);
        doc.add(uid);
        writer.updateDocument(new Term("_uid", "1"), doc);
        directoryReader = DirectoryReader.openIfChanged(directoryReader);
        atomicReader = SlowCompositeReaderWrapper.wrap(directoryReader);
        assertThat(UidField.loadVersion(atomicReader.getContext(), new Term("_uid", "1")), equalTo(2l));
        assertThat(UidField.loadDocIdAndVersion(atomicReader.getContext(), new Term("_uid", "1")).version, equalTo(2l));

        // test reuse of uid field
        doc = new Document();
        uid.version(3);
        doc.add(uid);
        writer.updateDocument(new Term("_uid", "1"), doc);
        directoryReader = DirectoryReader.openIfChanged(directoryReader);
        atomicReader = SlowCompositeReaderWrapper.wrap(directoryReader);
        assertThat(UidField.loadVersion(atomicReader.getContext(), new Term("_uid", "1")), equalTo(3l));
        assertThat(UidField.loadDocIdAndVersion(atomicReader.getContext(), new Term("_uid", "1")).version, equalTo(3l));

        writer.deleteDocuments(new Term("_uid", "1"));
        directoryReader = DirectoryReader.openIfChanged(directoryReader);
        atomicReader = SlowCompositeReaderWrapper.wrap(directoryReader);
        assertThat(UidField.loadVersion(atomicReader.getContext(), new Term("_uid", "1")), equalTo(-1l));
        assertThat(UidField.loadDocIdAndVersion(atomicReader.getContext(), new Term("_uid", "1")), nullValue());
    }
}
