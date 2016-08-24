package com.latticeengines.dataflow.exposed.builder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import edu.emory.mathcs.backport.java.util.Collections;

public class MetadataCascade {
    private static final Logger log = Logger.getLogger(MetadataCascade.class);

    private final List<FieldMetadata> metadata;

    public MetadataCascade(List<FieldMetadata> metadata) {
        this.metadata = metadata;
    }

    public void cascade() {
        for (FieldMetadata field : metadata) {
            List<FieldMetadata> ancestors = getAllAncestors(field);
            if (ancestors.size() > 0 && Iterables.all(ancestors, new Predicate<FieldMetadata>() {
                @Override
                public boolean apply(@Nullable FieldMetadata ancestor) {
                    Attribute attribute = new Attribute();
                    attribute.setApprovedUsage(ancestor.getPropertyValue("ApprovedUsage"));
                    if (attribute.getApprovedUsage().size() > 0) {
                        ApprovedUsage approvedUsage = ApprovedUsage.fromName(attribute.getApprovedUsage().get(0));
                        return approvedUsage == ApprovedUsage.NONE;
                    }
                    return true;
                }
            })) {
                List<String> names = DataFlowUtils.getFieldNames(ancestors);
                log.info(String
                        .format("Cascading down to set field %s ApprovedUsage to NONE because ancestors %s are all ApprovedUsage NONE",
                                field.getFieldName(), names));
                field.setPropertyValue("ApprovedUsage", Collections.singletonList(ApprovedUsage.NONE).toString());
            }
        }

    }

    private List<FieldMetadata> getAllAncestors(FieldMetadata field) {
        List<FieldMetadata> ancestors = flattenAncestry(field);
        eliminateDuplicateAncestors(ancestors);
        return ancestors;
    }

    private List<FieldMetadata> flattenAncestry(FieldMetadata field) {
        List<FieldMetadata> ancestors = new ArrayList<>();
        ancestors.addAll(field.getImmediateAncestors());
        for (FieldMetadata ancestor : field.getImmediateAncestors()) {
            ancestors.addAll(flattenAncestry(ancestor));
        }

        return ancestors;
    }

    private void eliminateDuplicateAncestors(List<FieldMetadata> ancestors) {
        Set<Pair<String, String>> set = new HashSet<>();

        ListIterator<FieldMetadata> iter = ancestors.listIterator();
        while (iter.hasNext()) {
            FieldMetadata metadata = iter.next();
            if (metadata.getTableName() != null) {
                Pair<String, String> pair = new ImmutablePair<>(metadata.getFieldName(), metadata.getTableName());
                if (set.contains(pair)) {
                    iter.remove();
                }
            }
        }
    }
}
