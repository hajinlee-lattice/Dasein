package com.latticeengines.apps.cdl.mds;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore2;

/**
 * TenantId, Version
 */
public interface CuratedContactAttrsMetadataStore extends MetadataStore2<String, DataCollection.Version> {
}
