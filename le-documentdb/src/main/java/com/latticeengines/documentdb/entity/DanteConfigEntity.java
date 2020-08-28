package com.latticeengines.documentdb.entity;

import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.latticeengines.domain.exposed.dante.DanteConfig;

@Entity
@Table(name = "DanteConfiguration", //
        indexes = { @Index(name = "IX_ID", columnList = "TenantId") }, //
        uniqueConstraints = { @UniqueConstraint(name = "UX_ID", columnNames = { "TenantId" }) })
public class DanteConfigEntity extends BaseMultiTenantDocEntity<DanteConfig> {

    @Override
    public DanteConfig getDocument() {
        DanteConfig config = super.getDocument();
        return config;
    }
}
