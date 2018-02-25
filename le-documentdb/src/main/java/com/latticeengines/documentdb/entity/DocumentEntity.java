package com.latticeengines.documentdb.entity;

public interface DocumentEntity<T> {

    String getUuid();

    void setUuid(String uuid);

    T getDocument();

    void setDocument(T dto);

}
