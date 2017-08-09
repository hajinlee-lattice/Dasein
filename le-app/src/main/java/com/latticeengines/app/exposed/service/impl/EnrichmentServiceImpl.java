package com.latticeengines.app.exposed.service.impl;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.EnrichmentService;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component("enrichmentService")
public class EnrichmentServiceImpl implements EnrichmentService {

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

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
        List<ColumnMetadata> cms = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Enrichment, "");
        Set<String> internalAttrs = cms.stream().filter(ColumnMetadata::isCanInternalEnrich)
                .map(ColumnMetadata::getColumnId).collect(Collectors.toSet());
        if (excludeInternalEnrichment) {
            topNTree.getCategories().forEach((cat, catTree) -> catTree.getSubcategories()
                    .forEach((subCat, attrs) -> attrs.removeIf(attr -> internalAttrs.contains(attr.getAttribute()))));
        }
        return topNTree;
    }
}
