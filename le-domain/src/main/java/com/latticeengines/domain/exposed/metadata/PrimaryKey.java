package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@Entity
@javax.persistence.Table(name = "METADATA_PRIMARY_KEY")
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class PrimaryKey extends AttributeOwner {

}
