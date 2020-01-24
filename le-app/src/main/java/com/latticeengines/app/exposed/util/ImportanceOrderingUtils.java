package com.latticeengines.app.exposed.util;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;

import reactor.core.publisher.Flux;

public final class ImportanceOrderingUtils {

    protected ImportanceOrderingUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Map<String, Integer> FIRMOGRAPHIC_ORDERING = new ImmutableMap.Builder<String, Integer>()
            .put("LDC_PrimaryIndustry", 100) //
            .put("LE_REVENUE_RANGE", 90) //
            .put("LE_EMPLOYEE_RANGE", 80) //
            .put("LDC_Domain", 70) //
            .put("LE_NUMBER_OF_LOCATIONS", 65) //
            .put("LDC_Country", 60) //
            .put("LDC_City", 50) //
            .put("LDC_State", 40) //
            .build();

    public static int getPredefinedImportanceOrder(String attrName) {
        return FIRMOGRAPHIC_ORDERING.getOrDefault(attrName, 0);
    }

    public static void addImportanceOrdering(List<ColumnMetadata> cms) {
        for (ColumnMetadata cm : cms) {
            if (Category.FIRMOGRAPHICS.equals(cm.getCategory())) {
                int importance = FIRMOGRAPHIC_ORDERING.getOrDefault(cm.getAttrName(), 0);
                cm.setImportanceOrdering(importance);
            }
        }
    }

    public static Flux<ColumnMetadata> addImportanceOrdering(Flux<ColumnMetadata> cms) {
        return cms.map(cm -> {
            if (Category.FIRMOGRAPHICS.equals(cm.getCategory())) {
                int importance = FIRMOGRAPHIC_ORDERING.getOrDefault(cm.getAttrName(), 0);
                cm.setImportanceOrdering(importance);
            }
            return cm;
        });
    }

    public static void addImportanceOrderingToLeadEnrichmentAttrs(List<LeadEnrichmentAttribute> attributes) {
        if (attributes == null) {
            return;
        }
        for (LeadEnrichmentAttribute attribute : attributes) {
            if (FIRMOGRAPHIC_ORDERING.containsKey(attribute.getFieldName())) {
                attribute.setImportanceOrdering(FIRMOGRAPHIC_ORDERING.get(attribute.getFieldName()));
            }
        }
    }

}
