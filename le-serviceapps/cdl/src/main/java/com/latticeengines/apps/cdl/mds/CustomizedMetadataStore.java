package com.latticeengines.apps.cdl.mds;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore4;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;

public interface CustomizedMetadataStore extends
        MetadataStore4<BusinessEntity, DataCollection.Version, StoreFilter, String> {
}
