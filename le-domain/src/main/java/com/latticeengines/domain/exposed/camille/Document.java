package com.latticeengines.domain.exposed.camille;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class Document {
    private byte[] data;
    private int version = -1;
    private DocumentMetadata metadata;

    public Document() {
    }

    public Document(byte[] data) {
        this.data = data;
    }

    public Document(byte[] data, DocumentMetadata metadata) {
        this.data = data;
        this.metadata = metadata;
    }

    public Document(byte[] data, DocumentMetadata metadata, int version) {
        if (version < 0) {
            throw new IllegalArgumentException("version must be greater or equal to 0");
        }

        this.data = data;
        this.metadata = metadata;
        this.version = version;
    }
    
    public Document(String data) throws Exception {
        this(data == null ? null : data.getBytes("UTF-8"));
    }
    
    public Document(String data, DocumentMetadata metadata) throws Exception {
        this(data == null ? null : data.getBytes("UTF-8"), metadata);
    }
    
    public Document(String data, DocumentMetadata metadata, int version) throws Exception {
        this(data == null ? null : data.getBytes("UTF-8"), metadata, version);
    }

    public byte[] getData() {
        return this.data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
    
    public String getString() throws Exception {
        return data == null ? null : new String(this.data, "UTF-8");
    }

    public void setString(String data) throws Exception{
        this.data = data == null ? null : data.getBytes("UTF-8");
    }

    public int getVersion() {
        return this.version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public DocumentMetadata getMetadata() {
        return this.metadata;
    }

    public void setMetadata(DocumentMetadata metadata) {
        this.metadata = metadata;
    }

    public boolean versionSpecified() {
        return this.version >= 0;
    }

    @Override
    public String toString() {
        return "Document [data=" + data + ", version=" + version + ", metadata=" + metadata + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((data == null) ? 0 : data.hashCode());
        result = prime * result + ((metadata == null) ? 0 : metadata.hashCode());
        result = prime * result + version;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Document other = (Document) obj;
        if (data == null) {
            if (other.data != null)
                return false;
        } else if (!Arrays.equals(data, other.data))
            return false;
        if (metadata == null) {
            if (other.metadata != null)
                return false;
        } else if (!metadata.equals(other.metadata))
            return false;
        if (version != other.version)
            return false;
        return true;
    }

}
