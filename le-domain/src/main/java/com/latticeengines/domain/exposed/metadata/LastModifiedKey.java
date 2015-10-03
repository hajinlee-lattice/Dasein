package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Entity;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Entity
@Table(name = "METADATA_LASTMODIFIED_KEY")
@PrimaryKeyJoinColumn(name = "PID")
@OnDelete(action = OnDeleteAction.CASCADE)
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class LastModifiedKey extends AttributeOwner {

    public LastModifiedKey() {
        setType(AttributeOwnerType.LASTMODIFIEDKEY.getValue());
    }
}
