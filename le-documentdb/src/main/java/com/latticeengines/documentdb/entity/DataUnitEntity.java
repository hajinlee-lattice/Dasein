package com.latticeengines.documentdb.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

@Entity
@Table(name = "DataUnit", //
        indexes = { @Index(name = "IX_NAME", columnList = "Name"),
                @Index(name = "IX_DATATEMPLATEID", columnList = "TenantId, DataTemplateId") }, //
        uniqueConstraints = {
                @UniqueConstraint(name = "UX_NAME_TYPE", columnNames = { "TenantId", "Name", "StorageType" }) })
public class DataUnitEntity extends BaseMultiTenantDocEntity<DataUnit> {

    @Column(name = "Name", //
            columnDefinition = "'VARCHAR(200) GENERATED ALWAYS AS (`Document` ->> '$.Name')'", //
            insertable = false, updatable = false)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(name = "StorageType", //
            columnDefinition = "'VARCHAR(20) GENERATED ALWAYS AS (`Document` ->> '$.StorageType')'", //
            insertable = false, updatable = false)
    private DataUnit.StorageType storageType;

    @Column(name = "DataTemplateId", //
            columnDefinition = "'VARCHAR(200) GENERATED ALWAYS AS (`Document` ->> '$.DataTemplateId')'", //
            insertable = false, updatable = false)
    private String dataTemplateId;

    @Column(name = "RetentionPolicy", //
            columnDefinition = "'VARCHAR(255) GENERATED ALWAYS AS (`Document` ->> '$.retentionPolicy')'", //
            insertable = false, updatable = false)
    private String retentionPolicy;

}
