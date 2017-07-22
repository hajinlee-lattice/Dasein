package com.latticeengines.app.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;

public interface EnrichmentService {

    void updateEnrichmentMatchFields(String id, List<MarketoMatchField> marketoMatchFields);

    StatsCube getStatsCube();

    TopNTree getTopNTree(boolean excludeInternalEnrichment);

}
