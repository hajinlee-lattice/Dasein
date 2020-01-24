package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;

public final class PivotMappingFileUtils {

    protected PivotMappingFileUtils() {
        throw new UnsupportedOperationException();
    }

    public static List<Attribute> createAttrsFromPivotSourceColumns(Set<String> sourceColumnNames,
            List<Attribute> attrs) {
        List<Attribute> newAttrs = new ArrayList<>();
        newAttrs.addAll(attrs);
        sourceColumnNames.stream().forEach(sc -> {
            attrs.stream()//
                    .filter(attr -> attr.getDisplayName().equalsIgnoreCase(sc))//
                    .findFirst() //
                    .ifPresent(attr -> attr.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS));
        });
        return newAttrs;
    }
}
