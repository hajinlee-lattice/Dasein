package com.latticeengines.metadata.mds;

import com.latticeengines.domain.exposed.metadata.mds.MetadataStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;

public interface NamedMetadataStore<N extends Namespace> extends MetadataStore<N> {

    String getName();

    N parseNameSpace(String... namespace);

    Long count(N namespace);

}
