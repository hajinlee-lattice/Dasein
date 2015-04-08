package com.latticeengines.domain.exposed.admin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class SerializableDocumentDirectory {

    private DocumentDirectory documentDirectory;

    public SerializableDocumentDirectory() {
    }
    
    public SerializableDocumentDirectory(DocumentDirectory documentDirectory) {
        this.setDocumentDirectory(documentDirectory);
    }

    @JsonIgnore
    public DocumentDirectory getDocumentDirectory() {
        return documentDirectory;
    }

    @JsonIgnore
    public void setDocumentDirectory(DocumentDirectory documentDirectory) {
        this.documentDirectory = documentDirectory;
    }

}
