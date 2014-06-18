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

//    @Temporal(TemporalType.TIMESTAMP)
//    @Column(name = "Updated", nullable = false)
//    private Date updated;

    public Date getCreated() {
        return created;
    }

//    public Date getUpdated() {
//        return updated;
//    }

    @PrePersist
    protected void onCreate() {
        /*updated = */created = new Date();
    }

//    @PreUpdate
//    protected void onUpdate() {
//        updated = new Date();
//    }
}