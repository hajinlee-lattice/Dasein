package com.latticeengines.apps.cdl.mds;

import com.latticeengines.domain.exposed.metadata.mds.MetadataStore2;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * TenantId, Entity
 */
public interface ExternalSystemMetadataStore extends MetadataStore2<String, BusinessEntity> {
}
