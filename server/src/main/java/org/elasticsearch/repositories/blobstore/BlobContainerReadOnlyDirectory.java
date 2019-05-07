package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BlobContainerReadOnlyDirectory extends BaseDirectory {

    private final String[] names;
    private final Map<String, BlobStoreIndexShardSnapshot.FileInfo> infoMap;
    private final BlobContainer blobContainer;

    public BlobContainerReadOnlyDirectory(SnapshotFiles snapshotFiles, BlobContainer blobContainer) {
        super(NoLockFactory.INSTANCE);
        this.blobContainer = blobContainer;
        infoMap = new HashMap<>();
        List<BlobStoreIndexShardSnapshot.FileInfo> fileInfos = snapshotFiles.indexFiles();
        names = new String[fileInfos.size()];
        int i = 0;
        for (BlobStoreIndexShardSnapshot.FileInfo info : fileInfos) {
            infoMap.put(info.name(), info);
            names[i++] = info.name();
        }
    }

    @Override
    public String[] listAll() {
        return names;
    }

    @Override
    public void deleteFile(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long fileLength(String name) throws IOException {
        BlobStoreIndexShardSnapshot.FileInfo info = infoMap.get(name);
        if (info == null) {
            throw new FileNotFoundException(name);
        }
        return info.length();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> names) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void syncMetaData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rename(String source, String dest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        BlobStoreIndexShardSnapshot.FileInfo info = infoMap.get(name);
        if (info == null) {
            throw new FileNotFoundException(name);
        }
        return indexInput(info, context);
    }

    @Override
    public void close() {
    }

    @Override
    public Set<String> getPendingDeletions() {
        return Collections.emptySet();
    }

    private IndexInput indexInput(BlobStoreIndexShardSnapshot.FileInfo fileInfo, IOContext context) throws IOException {
        long numParts = fileInfo.numberOfParts();
        if (numParts == 1) {
            return blobContainer.open(fileInfo.partName(0), fileInfo.length(), context);
        }
        return new PartsBufferedIndexInput(blobContainer, fileInfo, context);
    }

    private static class PartsBufferedIndexInput extends BufferedIndexInput {
        private final BlobStoreIndexShardSnapshot.FileInfo fileInfo;
        private final IOContext context;
        private final BlobContainer blobContainer;
        private IndexInput currentInput;
        private int currentPart;

        public PartsBufferedIndexInput(BlobContainer blobContainer, BlobStoreIndexShardSnapshot.FileInfo fileInfo, IOContext context) {
            super(fileInfo.name());
            this.blobContainer = blobContainer;
            this.fileInfo = fileInfo;
            this.context = context;
            currentPart = -1;
        }

        @Override
        public void close() throws IOException {
            org.elasticsearch.core.internal.io.IOUtils.close(currentInput);
        }

        @Override
        public long length() {
            return fileInfo.length();
        }

        @Override
        protected void readInternal(byte[] b, int offset, int length) throws IOException {
            final long filePointer = getFilePointer();
            int n = 0;
            while (n < length) {
                int count = readInternal1(b, offset + n, length - n, filePointer + n);
                assert count >= 0;
                n += count;
            }
        }

        private int readInternal1(byte[] b, int offset, int length, long filePointer) throws IOException {
            int part = (int) (filePointer / fileInfo.partSize().getBytes());
            if (part != currentPart) {
                currentPart = part;
                IOUtils.close(currentInput);
                currentInput = blobContainer.open(fileInfo.partName(part), fileInfo.partBytes(part), context);
            }
            long fp = currentInput.getFilePointer();
            long len = currentInput.length();
            int toRead = Math.min(Math.toIntExact(len - fp), length);
            currentInput.readBytes(b, offset, toRead, false);
            return toRead;
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            if (pos > length()) {
                throw new EOFException("read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
            }
        }

        @Override
        public BufferedIndexInput clone() {
            return new PartsBufferedIndexInput(blobContainer, fileInfo, context);
        }
    }

}
