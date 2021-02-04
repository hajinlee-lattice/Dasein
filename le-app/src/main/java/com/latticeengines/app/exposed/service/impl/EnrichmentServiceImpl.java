package com.latticeengines.app.exposed.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.app.exposed.service.EnrichmentService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component("enrichmentService")
public class EnrichmentServiceImpl implements EnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentServiceImpl.class);

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private BatonService batonService;

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
        String tenantId = MultiTenantContext.getShortTenantId();
        TopNTree topNTree = columnMetadataProxy.getTopNTree(excludeInternalEnrichment);
        TopNTree newTopNTree = new TopNTree();
        newTopNTree.setCategories(topNTree.getCategories());

        List<Category> categories = new ArrayList<>(newTopNTree.getCategories().keySet());
        for (Category cat : categories) {
            if (batonService.getMaxDataLicense(cat, tenantId) == 0) {
                newTopNTree.getCategories().remove(cat);
                log.info("Tenant=" + tenantId + ", catetory=" + cat + ", data cloud license is 0");
            }
        }
        return newTopNTree;
    }

}
