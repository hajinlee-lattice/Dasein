package com.latticeengines.domain.exposed.db;

import java.util.Date;

public interface HasAuditingFields {
    Date getCreated();

    void setCreated(Date date);

    Date getUpdated();

    void setUpdated(Date date);
}
