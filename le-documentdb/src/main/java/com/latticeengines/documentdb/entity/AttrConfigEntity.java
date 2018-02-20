package com.latticeengines.documentdb.entity;

import java.util.Arrays;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Index;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Entity
@Table(name = "AttributeConfiguration", //
        indexes = {@Index(name = "IX_NAMESPACE", columnList = "TenantId,Entity")})
public class AttrConfigEntity extends BaseColumnMetadataDocEntity implements MetadataEntity {

    private static final long serialVersionUID = 104466407857299607L;

    @JsonProperty("Entity")
    @Enumerated(EnumType.STRING)
    @Column(name = "Entity", nullable = false)
    private BusinessEntity entity;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    @Override
    public List<String> getnamespaceKeys() {
        return Arrays.asList(COLUMN_TENANT_ID, "Entity");
    }

}
