package com.latticeengines.app.exposed.service.impl;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.AMApiUpdate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.app.exposed.service.EnrichmentService;
import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component("enrichmentService")
public class EnrichmentServiceImpl implements EnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentServiceImpl.class);
    private static final String DUMMY_KEY = "DummyKey";

    private WatcherCache<String, Map<String, Boolean>> internalEnrichmentCache;

    @Autowired
    private AttributeService attributeService;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @SuppressWarnings("unchecked")
    @PostConstruct
    private void postConstruct() {
        internalEnrichmentCache = WatcherCache.builder() //
                .watch(AMApiUpdate) //
                .name("InternalEnrichmentCache") //
                .maximum(1) //
                .load(key -> {
                    if (DUMMY_KEY.equals(key)) {
                        log.info("Loaded internal enrichment into cache.");
                        List<LeadEnrichmentAttribute> allAttrs = attributeService.getAllAttributes();
                        Map<String, Boolean> updatedFlagMapForInternalEnrichment = new HashMap<>();
                        for (LeadEnrichmentAttribute attr : allAttrs) {
                            updatedFlagMapForInternalEnrichment.put(attr.getFieldName(), attr.getIsInternal());
                        }
                        return updatedFlagMapForInternalEnrichment;
                    } else {
                        return null;
                    }
                }) //
                .initKeys(new String[] { DUMMY_KEY }) //
                .build();
        internalEnrichmentCache.scheduleInit(13, TimeUnit.MINUTES);
    }

    @Override
    public void updateEnrichmentMatchFields(String id, List<MarketoMatchField> marketoMatchFields) {
    }

    @Override
    public StatsCube getStatsCube() {
        return columnMetadataProxy.getStatsCube();
    }

    @Override
    public TopNTree getTopNTree(boolean excludeInternalEnrichment) {
        TopNTree topNTree = columnMetadataProxy.getTopNTree();
        if (excludeInternalEnrichment) {
            Map<String, Boolean> internalEnrichmentFlags = internalEnrichmentCache.get(DUMMY_KEY);
            topNTree.getCategories()
                    .forEach((cat, catTree) -> catTree.getSubcategories().forEach((subCat, attrs) -> attrs
                            .removeIf(attr -> internalEnrichmentFlags.getOrDefault(attr.getAttribute(), false))));
        }
        return topNTree;
    }
}
