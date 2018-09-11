package com.latticeengines.apps.cdl.mds;

import com.latticeengines.domain.exposed.metadata.mds.MetadataStore1;

public interface RatingDisplayMetadataStore extends MetadataStore1<String> {
    String getSecondaryDisplayName(String suffix);
}
