package com.latticeengines.apps.cdl.mds;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace3;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;

/**
 * Metadata directly read from Tables
 */
public interface RawSystemMetadataStore extends MetadataStore<Namespace3<BusinessEntity, DataCollection.Version, StoreFilter>> {
}
