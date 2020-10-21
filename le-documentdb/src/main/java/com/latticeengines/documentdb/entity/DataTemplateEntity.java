package com.latticeengines.documentdb.entity;

import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

@Entity
@Table(name = "DataUnit", //
        indexes = { @Index(name = "IX_NAME", columnList = "Name") }, //
        uniqueConstraints = {
                @UniqueConstraint(name = "UX_TYPE", columnNames = { "TenantId"}) })
public class DataTemplateEntity extends BaseMultiTenantDocEntity<DataUnit> {


}
