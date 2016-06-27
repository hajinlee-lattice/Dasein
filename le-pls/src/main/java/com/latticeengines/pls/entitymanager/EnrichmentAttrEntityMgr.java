package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.pls.EnrichmentAttribute;

public interface EnrichmentAttrEntityMgr {

    List<EnrichmentAttribute> findAll();

    List<EnrichmentAttribute> upsert(List<EnrichmentAttribute> attributes);

}
