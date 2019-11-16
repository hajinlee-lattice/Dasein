package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Entity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Entity
@javax.persistence.Table(name = "METADATA_PRIMARY_KEY")
@JsonIgnoreProperties(value = { "hibernateLazyInitializer", "handler" }, ignoreUnknown = true)
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PrimaryKey extends AttributeOwner {

}
