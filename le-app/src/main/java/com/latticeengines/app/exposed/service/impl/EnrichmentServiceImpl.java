package com.latticeengines.app.exposed.service.impl;

import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.app.exposed.service.EnrichmentService;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component("enrichmentService")
public class EnrichmentServiceImpl implements EnrichmentService {

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @PostConstruct
    private void postConstruct() {
        columnMetadataProxy.scheduleLoadColumnMetadataCache();
    }

    @Override
    public void updateEnrichmentMatchFields(String id, List<MarketoMatchField> marketoMatchFields) {
    }

    @Override
    public StatsCube getStatsCube() {
        return columnMetadataProxy.getStatsCube();
    }

    @Override
    public Map<BusinessEntity, StatsCube> getStatsCubes() {
        return ImmutableMap.of(BusinessEntity.LatticeAccount, getStatsCube());
    }

    @Override
    public TopNTree getTopNTree(boolean excludeInternalEnrichment) {
        return columnMetadataProxy.getTopNTree(excludeInternalEnrichment);
    }
}
