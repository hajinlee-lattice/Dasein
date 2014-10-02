package com.latticeengines.domain.exposed.camille;

public class Document {
    private String data;
    private int version = -1;
    private DocumentMetadata metadata;
    
    public Document(String data, DocumentMetadata metadata) {
        this.data = data;
        this.metadata = metadata;
    }
    
    public Document(String data, DocumentMetadata metadata, int version) {
        if (version < 0) {
            throw new IllegalArgumentException("version must be greater or equal to 0");
        }        
        
        this.data = data;
        this.metadata = metadata;
        this.version = version;
    }
    
    public String getData() {
        return this.data;
    }
    
    public void setData(String data) {
        this.data = data;
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
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (getClass() != other.getClass())
            return false;
        
        Document otherDoc = (Document)other;
        return this.data.equals(otherDoc.data) &&
               this.metadata.equals(otherDoc.metadata) &&
               this.version == otherDoc.version;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime*result + this.data.hashCode();
        result = prime*result + this.metadata.hashCode();
        result = prime*result + this.version;
        return result;
    }
    
}
