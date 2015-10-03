package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Entity;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@Entity
@Table(name = "METADATA_PRIMARY_KEY")
@PrimaryKeyJoinColumn(name = "PID")
@OnDelete(action = OnDeleteAction.CASCADE)
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class PrimaryKey extends AttributeOwner {
    
    public PrimaryKey() {
        setType(AttributeOwnerType.PRIMARYKEY.getValue());
    }
}
