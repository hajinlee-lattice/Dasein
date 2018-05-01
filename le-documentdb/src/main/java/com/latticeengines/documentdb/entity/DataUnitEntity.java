package com.latticeengines.documentdb.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.documentdb.annotation.TenantIdColumn;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

@Entity
@Table(name = "DataUnit", indexes = { @Index(name = "IX_NAME", columnList = "TenantId,Name") })
public class DataUnitEntity extends BaseMultiTenantDocEntity<DataUnit> {

    @Column(name = "Name", //
            columnDefinition = "'VARCHAR(100) GENERATED ALWAYS AS (`Document` ->> '$.Name')'", //
            insertable = false, updatable = false)
    private String name;

    @Column(name = "StorageType", //
            columnDefinition = "'VARCHAR(20) GENERATED ALWAYS AS (`Document` ->> '$.StorageType')'", //
            insertable = false, updatable = false)
    private String storageType;

}
