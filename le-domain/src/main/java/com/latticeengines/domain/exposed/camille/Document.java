package com.latticeengines.domain.exposed.camille;

public class Document {
    private String data;
    private int version;
    private DocumentMetadata metadata;
    
    public Document(String data, DocumentMetadata metadata) {
        this.data = data;
        this.metadata = metadata;
    }
    
    public Document(String data, DocumentMetadata metadata, int version) {
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
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Document)) {
            return false;
        }
        
        Document otherDoc = (Document)other;
        return data.equals(otherDoc.data) &&
               metadata.equals(otherDoc.metadata) &&
               version == otherDoc.version;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime*result + data.hashCode();
        result = prime*result + metadata.hashCode();
        result = prime*result + version;
        return result;
    }
    
}
