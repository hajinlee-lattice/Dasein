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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.AttributeSetEntityMgr;
import com.latticeengines.apps.cdl.mds.AttributeSetDecoratorFac;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.ColumnMetadataUtils;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DummyDecorator;
import com.latticeengines.domain.exposed.metadata.mds.MapDecorator;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace3;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.AttributeUtils;
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
    public Decorator getDecorator(Namespace3<String, BusinessEntity, String> namespace) {
        final Tenant tenant = MultiTenantContext.getTenant();
        final BusinessEntity entity = namespace.getCoord2();
        final String attributeSetName = namespace.getCoord3();
        if (AttributeUtils.isDefaultAttributeSet(attributeSetName)) {
            return new DummyDecorator();
        } else {
            AttributeSet attributeSet = attributeSetEntityMgr.findByName(attributeSetName);
            if (attributeSet == null) {
                log.info("Attribute set can't be found, so default attribute set will be used.");
                return new DummyDecorator();
            }
            return new MapDecorator("AttrSet") {
                @Override
                protected Collection<ColumnMetadata> loadInternal() {
                    log.info("Load attribute set by name {}.", attributeSetName);
                    MultiTenantContext.setTenant(tenant);
                    if (MapUtils.isNotEmpty(attributeSet.getAttributesMap())) {
                        Map<String, Set<String>> attributesMap = attributeSet.getAttributesMap();
                        Set<String> attributes = new HashSet<>();
                        if (BusinessEntity.Account.equals(entity)) {
                            for (Category category : Category.values()) {
                                if (!category.isHiddenFromUi()) {
                                    if (BusinessEntity.Account.equals(CategoryUtils.getEntity(category).get(0))) {
                                        addCategoryAttributes(attributes, attributesMap, category);
                                    }
                                }
                            }
                        } else {
                            Category category = CategoryUtils.getEntityCategory(entity);
                            addCategoryAttributes(attributes, attributesMap, category);
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
