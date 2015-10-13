package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Entity
@javax.persistence.Table(name = "METADATA_LASTMODIFIED_KEY")
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class LastModifiedKey extends AttributeOwner {

}
