package com.latticeengines.domain.exposed.admin;

import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class SerializableDocumentDirectory {

    private DocumentDirectory documentDirectory;

    public SerializableDocumentDirectory() {
    }
    
    public SerializableDocumentDirectory(DocumentDirectory documentDirectory) {
        this.setDocumentDirectory(documentDirectory);
    }

    public DocumentDirectory getDocumentDirectory() {
        return documentDirectory;
    }

    public void setDocumentDirectory(DocumentDirectory documentDirectory) {
        this.documentDirectory = documentDirectory;
    }

}
