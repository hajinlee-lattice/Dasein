package com.latticeengines.app.exposed.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface EnrichmentService {

    void updateEnrichmentMatchFields(String id, List<MarketoMatchField> marketoMatchFields);

    StatsCube getStatsCube();

    Map<BusinessEntity, StatsCube> getStatsCubes();

    TopNTree getTopNTree(boolean excludeInternalEnrichment);

}
