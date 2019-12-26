package com.latticeengines.apps.cdl.mds;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore3;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;

public interface CustomizedMetadataStore extends MetadataStore3<BusinessEntity, DataCollection.Version, StoreFilter> {
}
