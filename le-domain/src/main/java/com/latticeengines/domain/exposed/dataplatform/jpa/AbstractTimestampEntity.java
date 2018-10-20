package com.latticeengines.domain.exposed.dataplatform.jpa;

import java.util.Date;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@MappedSuperclass
@Access(AccessType.FIELD)
public abstract class AbstractTimestampEntity {

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "Created", nullable = false)
    private Date created = new Date();

    public Date getCreated() {
        return created;
    }

    @PrePersist
    protected void onCreate() {
        /* updated = */created = new Date();
    }

}
