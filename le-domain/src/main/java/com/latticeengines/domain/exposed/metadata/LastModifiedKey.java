package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Entity;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

@Entity
@Table(name = "METADATA_LASTMODIFIED_KEY")
@PrimaryKeyJoinColumn(name = "PID")
@OnDelete(action = OnDeleteAction.CASCADE)
public class LastModifiedKey extends AttributeOwner {

    public LastModifiedKey() {
        setType(AttributeOwnerType.LASTMODIFIEDKEY.getValue());
    }
}
