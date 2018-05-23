package com.latticeengines.apps.cdl.mds;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore2;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface CustomizedMetadataStore extends MetadataStore2<BusinessEntity, DataCollection.Version> {
}
