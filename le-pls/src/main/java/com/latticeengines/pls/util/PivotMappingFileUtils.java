package com.latticeengines.pls.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;

public class PivotMappingFileUtils {

    public static List<Attribute> createAttrsFromPivotSourceColumns(Set<String> sourceColumnNames, List<Attribute> attrs) {
        List<Attribute> newAttrs = new ArrayList<>();
        newAttrs.addAll(attrs);
        for (final String sourceColumnName : sourceColumnNames) {
            Iterables.find(attrs, new Predicate<Attribute>() {
                @Override
                public boolean apply(Attribute input) {
                    if (input.getDisplayName().equalsIgnoreCase(sourceColumnName)) {
                        input.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
                        return true;
                    }
                    return false;
                }

            }, null);
        }
        return newAttrs;
    }
}
