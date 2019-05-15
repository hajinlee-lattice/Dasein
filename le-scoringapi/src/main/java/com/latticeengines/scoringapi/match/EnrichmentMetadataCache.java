package com.latticeengines.scoringapi.match;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;

public interface EnrichmentMetadataCache {

    List<LeadEnrichmentAttribute> getEnrichmentAttributesMetadata(CustomerSpace space);

    List<LeadEnrichmentAttribute> getAllEnrichmentAttributesMetadata();

}
