package com.latticeengines.domain.exposed.db;

public interface HasAuditUser {

    String getCreatedBy();

    void setCreatedBy(String createdBy);
}
