package com.latticeengines.documentdb.entity;

import java.util.List;

public interface NameSpacedDocument<T> extends DocumentEntity<T> {

    List<String> getnamespaceKeys();

}
