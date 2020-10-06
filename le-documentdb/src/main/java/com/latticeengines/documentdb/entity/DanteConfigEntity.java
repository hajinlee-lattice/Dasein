package com.latticeengines.documentdb.entity;

import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Persistable;

import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;

@Entity
@Table(name = "DanteConfiguration", //
        indexes = { @Index(name = "IX_ID", columnList = "TenantId") }, //
        uniqueConstraints = { @UniqueConstraint(name = "UX_ID", columnNames = { "TenantId", "UUID" }) })
public class DanteConfigEntity extends BaseMultiTenantDocEntity<DanteConfigurationDocument>
        implements DocumentEntity<DanteConfigurationDocument>, Persistable<String> {

    @Override
    public DanteConfigurationDocument getDocument() {
        DanteConfigurationDocument config = super.getDocument();
        return config;
    }

    @Override
    public boolean isNew() {
        return StringUtils.isBlank(getUuid());
    }

    @Override
    public String getId() {
        return getTenantId();
    }
}
