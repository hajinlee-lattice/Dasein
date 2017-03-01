package com.latticeengines.flink.framework.source;

import org.apache.avro.file.SeekableInput;
import org.apache.flink.core.fs.FSDataInputStream;

import java.io.Closeable;
import java.io.IOException;

public class FSDataInputStreamWrapper implements Closeable, SeekableInput {
    private final FSDataInputStream stream;
    private long pos;
    private long len;

    public FSDataInputStreamWrapper(FSDataInputStream stream, long len) {
        this.stream = stream;
        this.pos = 0L;
        this.len = len;
    }

    public long length() throws IOException {
        return this.len;
    }

    public int read(byte[] b, int off, int len) throws IOException {
        int read = this.stream.read(b, off, len);
        this.pos += (long)read;
        return read;
    }

    public void seek(long p) throws IOException {
        this.stream.seek(p);
        this.pos = p;
    }

    public long tell() throws IOException {
        return this.pos;
    }

    public void close() throws IOException {
        this.stream.close();
    }
}
