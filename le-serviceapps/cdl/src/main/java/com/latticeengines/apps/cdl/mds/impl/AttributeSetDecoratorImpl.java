package com.latticeengines.apps.cdl.mds.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.AttributeSetEntityMgr;
import com.latticeengines.apps.cdl.mds.AttributeSetDecoratorFac;
import com.latticeengines.apps.cdl.util.AttributeSetContext;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.ColumnMetadataUtils;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DummyDecorator;
import com.latticeengines.domain.exposed.metadata.mds.MapDecorator;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.CategoryUtils;

@Component
public class AttributeSetDecoratorImpl implements AttributeSetDecoratorFac {

    private static final Logger log = LoggerFactory.getLogger(AttributeSetDecoratorImpl.class);

    private final AttributeSetEntityMgr attributeSetEntityMgr;

    @Inject
    public AttributeSetDecoratorImpl(AttributeSetEntityMgr attributeSetEntityMgr) {
        this.attributeSetEntityMgr = attributeSetEntityMgr;
    }

    @Override
    public Decorator getDecorator(Namespace2<String, BusinessEntity> namespace) {
        final Tenant tenant = MultiTenantContext.getTenant();
        final BusinessEntity entity = namespace.getCoord2();
        final String attributeSetName = AttributeSetContext.getAttributeSetName();
        if (StringUtils.isEmpty(attributeSetName)) {
            return new DummyDecorator();
        } else {
            return new MapDecorator("AttrSet") {
                @Override
                protected Collection<ColumnMetadata> loadInternal() {
                    log.info("Load attribute set by name {}.", attributeSetName);
                    MultiTenantContext.setTenant(tenant);
                    AttributeSet attributeSet = attributeSetEntityMgr.findByName(attributeSetName);
                    if (attributeSet != null && MapUtils.isNotEmpty(attributeSet.getAttributesMap())) {
                        Map<String, Set<String>> attributesMap = attributeSet.getAttributesMap();
                        Category category = CategoryUtils.getEntityCategory(entity);
                        Set<String> attributes = new HashSet<>();
                        addCategoryAttributes(attributes, attributesMap, category);
                        if (BusinessEntity.Account.equals(entity)) {
                            addCategoryAttributes(attributes, attributesMap, Category.FIRMOGRAPHICS);
                        }
                        return attributes.stream().map(attribute -> toColumnMetadata(attribute)).collect(Collectors.toList());
                    } else {
                        return Collections.emptyList();
                    }
                }

                @Override
                protected ColumnMetadata process(ColumnMetadata cm, AtomicLong counter) {
                    if (filterMap.containsKey(cm.getAttrName()) && !Boolean.FALSE.equals(cm.getCanEnrich())) {
                        counter.incrementAndGet();
                        cm = ColumnMetadataUtils.overwrite(filterMap.get(cm.getAttrName()), cm);
                    } else {
                        cm.disableGroup(ColumnSelection.Predefined.Enrichment);
                    }
                    return cm;
                }
            };
        }
    }

    private void addCategoryAttributes(Set<String> attributes, Map<String, Set<String>> attributesMap, Category category) {
        Set<String> attributesInCategory = attributesMap.get(category.name());
        if (CollectionUtils.isNotEmpty(attributesInCategory)) {
            attributes.addAll(attributesInCategory);
        }
    }

    private ColumnMetadata toColumnMetadata(String attributeName) {
        ColumnMetadata cm = new ColumnMetadata();
        cm.setAttrName(attributeName);
        cm.enableGroup(ColumnSelection.Predefined.Enrichment);
        return cm;
    }
}
