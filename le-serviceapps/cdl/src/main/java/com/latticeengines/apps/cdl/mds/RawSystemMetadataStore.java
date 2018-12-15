package com.latticeengines.apps.cdl.mds;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * Metadata directly read from Tables
 */
public interface RawSystemMetadataStore extends MetadataStore<Namespace2<BusinessEntity, DataCollection.Version>> {
}
