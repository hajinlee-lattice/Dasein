package com.latticeengines.apps.cdl.mds;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore2;

/**
 * TenantId, Version
 */
public interface CuratedAttrsMetadataStore extends MetadataStore2<String, DataCollection.Version> {
}
