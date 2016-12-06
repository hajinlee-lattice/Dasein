package com.latticeengines.datacloud.core.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.CategoricalAttributeEntityMgr;
import com.latticeengines.datacloud.core.service.DimensionalQueryService;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.DimensionalQuery;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("dimensionalQueryService")
public class DimensionalQueryServiceImpl implements DimensionalQueryService {

    @Autowired
    private CategoricalAttributeEntityMgr attributeEntityMgr;

    @Override
    public Long findAttrId(DimensionalQuery query) {
        String queryStr = query != null ? query.toString() : "null";
        if (query == null || StringUtils.isEmpty(query.getSource()) || StringUtils.isEmpty(query.getDimension())
                || query.getQualifiers() == null || query.getQualifiers().isEmpty()) {
            throw new LedpException(LedpCode.LEDP_25029, new String[] { queryStr, "Invalid query" });
        }

        CategoricalAttribute rootAttribute = attributeEntityMgr.getRootAttribute(query.getSource(),
                query.getDimension());
        if (rootAttribute == null) {
            throw new LedpException(LedpCode.LEDP_25028, new String[] { query.getSource(), query.getDimension() });
        }

        Map<String, String> qualifiers = query.getQualifiers();
        if (isDimensionRoot(rootAttribute, qualifiers)) {
            if (qualifiers.size() > 1) {
                throw new LedpException(LedpCode.LEDP_25029, new String[] { queryStr, "Too many qualifiers" });
            } else {
                return rootAttribute.getPid();
            }
        }

        // remove all qualifiers with the value _ALL_
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, String> entry : qualifiers.entrySet()) {
            if (CategoricalAttribute.ALL.equals(entry.getValue())) {
                toRemove.add(entry.getKey());
            }
        }
        for (String attrName : toRemove) {
            qualifiers.remove(attrName);
        }

        CategoricalAttribute attribute = findAttrRecursively(rootAttribute, qualifiers);
        if (attribute == null) {
            throw new LedpException(LedpCode.LEDP_25029,
                    new String[] { queryStr, "Cannot find any qualified attribute." });
        }
        return attribute.getPid();
    }

    private boolean isDimensionRoot(CategoricalAttribute rootAttribute, Map<String, String> qualifier) {
        return qualifier.containsKey(rootAttribute.getAttrName())
                && rootAttribute.getAttrValue().equals(qualifier.get(rootAttribute.getAttrName()));
    }

    private CategoricalAttribute findAttrRecursively(CategoricalAttribute currentAttr, Map<String, String> qualifiers) {
        if (qualifiers.containsKey(currentAttr.getAttrName())
                && currentAttr.getAttrValue().equals(qualifiers.get(currentAttr.getAttrName()))) {
            qualifiers.remove(currentAttr.getAttrName());
            if (qualifiers.isEmpty()) {
                return currentAttr;
            }
        }
        List<CategoricalAttribute> children = attributeEntityMgr.getChildren(currentAttr.getPid());
        for (CategoricalAttribute child : children) {
            CategoricalAttribute attribute = findAttrRecursively(child, cloneQualifiers(qualifiers));
            if (attribute != null) {
                return attribute;
            }
        }
        return null;
    }

    // clone is required to detect ambiguous paths: more than one paths satisfying the qualifiers
    // if we assume no ambiguity and want to remove the performance impact from clone
    // we can skip clone.
    private Map<String, String> cloneQualifiers(Map<String, String> qualifiers) {
        return new HashMap<>(qualifiers);
    }
}
