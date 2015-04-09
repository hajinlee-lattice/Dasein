package com.latticeengines.domain.exposed.admin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class SerializableDocumentDirectory {
    
    private String rootPath;

    private DocumentDirectory documentDirectory;

    public SerializableDocumentDirectory() {
    }
    
    public SerializableDocumentDirectory(DocumentDirectory documentDirectory) {
        this.setDocumentDirectory(documentDirectory);
        this.rootPath = documentDirectory.getRootPath().toString();
    }

    @JsonIgnore
    public DocumentDirectory getDocumentDirectory() {
        return documentDirectory;
    }

    @JsonIgnore
    public void setDocumentDirectory(DocumentDirectory documentDirectory) {
        this.documentDirectory = documentDirectory;
    }

    @JsonProperty("RootPath")
    public String getRootPath() {
        return rootPath;
    }

    @JsonProperty("RootPath")
    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

}
