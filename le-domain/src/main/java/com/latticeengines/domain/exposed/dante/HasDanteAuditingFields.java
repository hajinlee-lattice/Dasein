package com.latticeengines.domain.exposed.dante;

import java.util.Date;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

public interface HasDanteAuditingFields extends HasPid {
    String getCustomerID();

    void setCustomerID(String customerID);

    Date getCreationDate();

    void setCreationDate(Date creationDate);

    Date getLastModificationDate();

    void setLastModificationDate(Date lastModificationDate);
}
