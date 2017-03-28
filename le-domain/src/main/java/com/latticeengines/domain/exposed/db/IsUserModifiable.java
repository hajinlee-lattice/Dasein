package com.latticeengines.domain.exposed.db;

public interface IsUserModifiable {

    String getLastModifiedByUser();

    void setLastModifiedByUser(String lastModifiedByUser);

}
