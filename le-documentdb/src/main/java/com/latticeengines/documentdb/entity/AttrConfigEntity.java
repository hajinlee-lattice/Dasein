package com.latticeengines.documentdb.entity;

import java.util.Arrays;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;

@Entity
@Table(name = "AttributeConfiguration", //
        indexes = { @Index(name = "IX_NAMESPACE", columnList = "TenantId,Entity") }, //
        uniqueConstraints = { @UniqueConstraint(name = "UX_NAME", columnNames = { "TenantId", "Entity", "AttrName" }) })
public class AttrConfigEntity extends BaseMultiTenantDocEntity<AttrConfig>
        implements ColumnMetadataDocument<AttrConfig> {

    @Enumerated(EnumType.STRING)
    @Column(name = "Entity", //
            columnDefinition = "'VARCHAR(30) GENERATED ALWAYS AS (`Document` ->> '$.Entity')'", //
            insertable = false, updatable = false)
    private BusinessEntity entity;

    @Column(name = "AttrName", //
            columnDefinition = "'VARCHAR(100) GENERATED ALWAYS AS (`Document` ->> '$." + ColumnMetadataKey.AttrName
                    + "')'", //
            insertable = false, updatable = false)
    private String attrName;

    @Override
    public List<String> getnamespaceKeys() {
        return Arrays.asList(getTenantIdField(), "entity");
    }

    @Override
    public AttrConfig getDocument() {
        AttrConfig config = super.getDocument();
        config.fixJsonDeserialization();
        return config;
    }

}
