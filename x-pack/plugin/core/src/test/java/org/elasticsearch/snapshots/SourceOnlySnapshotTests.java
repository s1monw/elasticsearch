/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class SourceOnlySnapshotTests extends ESTestCase {
    public void testSourceOnlyRandom() throws IOException {
        try (Directory dir = newDirectory(); Directory targetDir = newDirectory()) {
            SnapshotDeletionPolicy deletionPolicy = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig().setIndexDeletionPolicy
                (deletionPolicy))) {
                boolean modifyDeletedDocs = true; randomBoolean();
                SourceOnlySnapshot snapshoter = new SourceOnlySnapshot(targetDir, writer.w.getConfig().getSoftDeletesField(),
                    modifyDeletedDocs? () -> new DocValuesFieldExistsQuery("some_values") : null);
                writer.commit();
                int numDocs = scaledRandomIntBetween(100, 10000);
                boolean appendOnly = randomBoolean();
                for (int i = 0; i < numDocs; i++) {
                    int docId = appendOnly ? i : randomIntBetween(0, 100);
                    Document d = newRandomDocument(docId);
                    if (appendOnly) {
                        writer.addDocument(d);
                    } else {
                        writer.updateDocument(new Term("id", Integer.toString(docId)), d);
                    }
                    if (rarely()) {
                        if (randomBoolean()) {
                            writer.commit();
                        }
                        IndexCommit snapshot = deletionPolicy.snapshot();
                        try {
                            snapshoter.syncSnapshot(snapshot);
                        } finally {
                            deletionPolicy.release(snapshot);
                        }
                    }
                }
                if (randomBoolean()) {
                    writer.commit();
                }
                IndexCommit snapshot = deletionPolicy.snapshot();
                try {
                    snapshoter.syncSnapshot(snapshot);
                    try (DirectoryReader snapReader = snapshoter.wrapReader(DirectoryReader.open(targetDir));
                         DirectoryReader wrappedReader = snapshoter.wrapReader(DirectoryReader.open(snapshot))) {
                         DirectoryReader reader = modifyDeletedDocs ? new SoftDeletesDirectoryReaderWrapper(wrappedReader, "some_value") :
                             wrappedReader;
                        assertEquals(snapReader.maxDoc(), reader.maxDoc());
                        assertEquals(snapReader.numDocs(), reader.numDocs());
                        for (int i = 0; i < snapReader.maxDoc(); i++) {
                            assertEquals(snapReader.document(i).get("_source"), reader.document(i).get("_source"));
                        }
                    }
                } finally {
                    deletionPolicy.release(snapshot);
                }
            }
        }
    }

    private Document newRandomDocument(int id) {
        Document doc = new Document();
        doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));
        doc.add(new NumericDocValuesField("id", id));
        if (randomBoolean()) {
            doc.add(new TextField("text", "the quick brown fox", Field.Store.NO));
        }
        if (randomBoolean()) {
            doc.add(new FloatPoint("float_point", 1.3f, 3.4f));
        }
        if (randomBoolean()) {
            doc.add(new NumericDocValuesField("some_value", randomLong()));
        }
        doc.add(new StoredField("_source", randomRealisticUnicodeOfCodepointLengthBetween(5, 10)));
        return doc;
    }

    public void testSrcOnlySnap() throws IOException {
        try (Directory dir = newDirectory()) {
            SnapshotDeletionPolicy deletionPolicy = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
            IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig()
                .setSoftDeletesField("id")
                .setIndexDeletionPolicy(deletionPolicy).setMergePolicy(new FilterMergePolicy(NoMergePolicy.INSTANCE) {
                    @Override
                    public boolean useCompoundFile(SegmentInfos infos, SegmentCommitInfo mergedInfo, MergeContext mergeContext) {
                        return randomBoolean();
                    }
                }));
            Document doc = new Document();
            doc.add(new StringField("id", "1", Field.Store.YES));
            doc.add(new TextField("text", "the quick brown fox", Field.Store.NO));
            doc.add(new NumericDocValuesField("rank", 1));
            doc.add(new StoredField("src", "the quick brown fox"));
            writer.addDocument(doc);
            doc = new Document();
            doc.add(new StringField("id", "2", Field.Store.YES));
            doc.add(new TextField("text", "the quick blue fox", Field.Store.NO));
            doc.add(new NumericDocValuesField("rank", 2));
            doc.add(new StoredField("src", "the quick blue fox"));
            doc.add(new StoredField("dummy", "foo")); // add a field only this segment has
            writer.addDocument(doc);
            writer.flush();
            doc = new Document();
            doc.add(new StringField("id", "1", Field.Store.YES));
            doc.add(new TextField("text", "the quick brown fox", Field.Store.NO));
            doc.add(new NumericDocValuesField("rank", 3));
            doc.add(new StoredField("src", "the quick brown fox"));
            writer.softUpdateDocument(new Term("id", "1"), doc, new NumericDocValuesField("id", 1));
            writer.commit();
            IndexCommit snapshot = deletionPolicy.snapshot();
            Directory targetDir = newDirectory();
            SourceOnlySnapshot snapshoter = new SourceOnlySnapshot(targetDir, "id");
            snapshoter.syncSnapshot(snapshot);


            StandardDirectoryReader  reader = (StandardDirectoryReader) DirectoryReader.open(snapshot);
            try(DirectoryReader snapReader = DirectoryReader.open(targetDir)) {
                assertEquals(snapReader.maxDoc(), 3);
                assertEquals(snapReader.numDocs(), 2);
                for (int i = 0; i < 3; i++) {
                    assertEquals(snapReader.document(i).get("src"), reader.document(i).get("src"));
                }
                IndexSearcher searcher = new IndexSearcher(snapReader);
                TopDocs id = searcher.search(new TermQuery(new Term("id", "1")), 10);
                assertEquals(0, id.totalHits);
            }

            snapshoter = new SourceOnlySnapshot(targetDir, "id");
            snapshoter.syncSnapshot(snapshot);
            assertEquals(0, snapshoter.getCreatedFiles().size());
            deletionPolicy.release(snapshot);
            // now add another doc
            doc = new Document();
            doc.add(new StringField("id", "4", Field.Store.YES));
            doc.add(new TextField("text", "the quick blue fox", Field.Store.NO));
            doc.add(new NumericDocValuesField("rank", 2));
            doc.add(new StoredField("src", "the quick blue fox"));
            writer.addDocument(doc);
            doc = new Document();
            doc.add(new StringField("id", "5", Field.Store.YES));
            doc.add(new TextField("text", "the quick blue fox", Field.Store.NO));
            doc.add(new NumericDocValuesField("rank", 2));
            doc.add(new StoredField("src", "the quick blue fox"));
            writer.addDocument(doc);
            writer.commit();
            {
                snapshot = deletionPolicy.snapshot();
                snapshoter = new SourceOnlySnapshot(targetDir, "id");
                snapshoter.syncSnapshot(snapshot);
                assertEquals(4, snapshoter.getCreatedFiles().size());
                for (String file : snapshoter.getCreatedFiles()) {
                    String extension = IndexFileNames.getExtension(file);
                    switch (extension) {
                        case "fdt":
                        case "fdx":
                        case "fnm":
                        case "si":
                            break;
                        default:
                            fail("unexpected extension: " + extension);
                    }
                }
                try(DirectoryReader snapReader = DirectoryReader.open(targetDir)) {
                    assertEquals(snapReader.maxDoc(), 5);
                    assertEquals(snapReader.numDocs(), 4);
                }
                deletionPolicy.release(snapshot);
            }
            writer.deleteDocuments(new Term("id", "5"));
            writer.commit();
            {
                snapshot = deletionPolicy.snapshot();
                snapshoter = new SourceOnlySnapshot(targetDir, "id");
                snapshoter.syncSnapshot(snapshot);
                assertEquals(1, snapshoter.getCreatedFiles().size());
                for (String file : snapshoter.getCreatedFiles()) {
                    String extension = IndexFileNames.getExtension(file);
                    switch (extension) {
                        case "liv":
                            break;
                        default:
                            fail("unexpected extension: " + extension);
                    }
                }
                try(DirectoryReader snapReader = DirectoryReader.open(targetDir)) {
                    assertEquals(snapReader.maxDoc(), 5);
                    assertEquals(snapReader.numDocs(), 3);
                }
                deletionPolicy.release(snapshot);
            }
            writer.close();
            targetDir.close();
            reader.close();
        }
    }
}
