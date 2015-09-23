package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Entity;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;

import org.hibernate.annotations.Filter;


@Entity
@Table(name = "METADATA_PRIMARY_KEY")
@PrimaryKeyJoinColumn(name = "PID")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class PrimaryKey extends AttributeOwner {
}
