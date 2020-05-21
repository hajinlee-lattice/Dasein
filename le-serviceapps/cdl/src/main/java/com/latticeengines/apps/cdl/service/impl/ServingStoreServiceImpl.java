package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.mds.CustomizedMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.util.AttributeUtils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@Service("servingStoreService")
public class ServingStoreServiceImpl implements ServingStoreService {

    @Inject
    private CustomizedMetadataStore customizedMetadataStore;

    @Inject
    private SystemMetadataStore systemMetadataStore;

    @Inject
    private DataCollectionService dataCollectionService;

    private static final Logger log = LoggerFactory.getLogger(ServingStoreServiceImpl.class);

    @Override
    public ParallelFlux<ColumnMetadata> getSystemMetadata(BusinessEntity entity, DataCollection.Version version) {
        return systemMetadataStore.getMetadataInParallel(entity, version, StoreFilter.ALL);
    }

    @Override
    public ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity,
            DataCollection.Version version) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return getFullyDecoratedMetadata(customerSpace, entity, version, StoreFilter.ALL);
    }

    @Override
    public ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity, DataCollection.Version version,
            StoreFilter filter, String attributeSetName) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return getFullyDecoratedMetadata(customerSpace, entity, version, filter, attributeSetName);
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.ServingMetadataCacheName, key = "T(java.lang.String).format(\"%s|%s|md_in_svc\", #tenantId, #entity)", unless = "#result == null")
    public List<ColumnMetadata> getDecoratedMetadataFromCache(String tenantId, BusinessEntity entity) {
        String customerSpace = CustomerSpace.parse(tenantId).toString();
        DataCollection.Version version = dataCollectionService.getActiveVersion(customerSpace);
        return getFullyDecoratedMetadata(customerSpace, entity, version, StoreFilter.ALL).sequential().collectList()
                .block();
    }

    private boolean isEnrichmentGroup(Collection<ColumnSelection.Predefined> groups) {
        return CollectionUtils.isNotEmpty(groups) && groups.size() == 1 && groups.contains(ColumnSelection.Predefined.Enrichment);
    }

    @Override
    public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity, DataCollection.Version version,
                                                     Collection<ColumnSelection.Predefined> groups, String attributeSetName, StoreFilter filter) {
        AtomicLong timer = new AtomicLong();
        AtomicLong counter = new AtomicLong();
        filter = filter == null ? StoreFilter.ALL : filter;

        if (AttributeUtils.isDefaultAttributeSet(attributeSetName) || !isEnrichmentGroup(groups)) {
            attributeSetName = null;
        }
        if (version == null) {
            version = dataCollectionService.getActiveVersion(customerSpace);
        }
        ParallelFlux<ColumnMetadata> flux = getFullyDecoratedMetadata(entity, version, filter, attributeSetName);
        flux = flux //
                .doOnSubscribe(s -> {
                    timer.set(System.currentTimeMillis());
                    log.info("Start serving decorated metadata for " + customerSpace + ":" + entity);
                }) //
                .doOnNext(cm -> counter.getAndIncrement()) //
                .doOnComplete(() -> {
                    long duration = System.currentTimeMillis() - timer.get();
                    log.info("Finished serving decorated metadata for " + counter.get() + " attributes from "
                            + customerSpace + ":" + entity + " TimeElapsed=" + duration + " msec");
                });
        Set<ColumnSelection.Predefined> filterGroups = new HashSet<>();
        if (CollectionUtils.isNotEmpty(groups)) {
            filterGroups.addAll(groups);
        }
        if (CollectionUtils.isNotEmpty(filterGroups)) {
            flux = flux.filter(cm -> filterGroups.stream().anyMatch(cm::isEnabledFor));
        }
        return flux.sequential();
    }

    @Override
    public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
            DataCollection.Version version, Collection<ColumnSelection.Predefined> groups) {
        return getDecoratedMetadata(customerSpace, entity, version, groups, null, StoreFilter.ALL);
    }

    @Override
    public Map<String, Boolean> getAttributesUsage(String customerSpace, BusinessEntity entity, Set<String> attributes,
            ColumnSelection.Predefined group, DataCollection.Version version) {
        if (CollectionUtils.isEmpty(attributes)) {
            return Collections.<String, Boolean> emptyMap();
        }

        Map<String, Boolean> result = attributes.stream().collect(Collectors.toMap(Function.identity(), attr -> {
            return false;
        }));

        List<String> cms = getDecoratedMetadata(customerSpace, entity, version, Collections.singletonList(group))
                .map(attr -> attr.getAttrName()).collectList().block();
        if (CollectionUtils.isNotEmpty(cms)) {
            attributes.stream().forEach(attr -> {
                if (cms.contains(attr)) {
                    result.put(attr, true);
                }
            });
        }
        return result;
    }

    @Override
    public List<ColumnMetadata> getAccountMetadata(String customerSpace, ColumnSelection.Predefined group,
            DataCollection.Version version) {
        return getDecoratedMetadataWithDeflatedDisplayName(customerSpace,
                BusinessEntity.getAccountExportEntities(group), version, Collections.singleton(group));
    }

    @Override
    public List<ColumnMetadata> getContactMetadata(String customerSpace, ColumnSelection.Predefined group,
            DataCollection.Version version) {
        return getDecoratedMetadataWithDeflatedDisplayName(customerSpace,
                Collections.singletonList(BusinessEntity.Contact), version, Collections.singleton(group));
    }

    private List<ColumnMetadata> getDecoratedMetadataWithDeflatedDisplayName(String customerSpace,
            Collection<BusinessEntity> entities, DataCollection.Version version,
            Collection<ColumnSelection.Predefined> groups) {
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        Tenant tenant = MultiTenantContext.getTenant();
        Map<BusinessEntity, List<ColumnMetadata>> map = entities.stream().parallel()
                .collect(Collectors.toMap(entity -> entity, entity -> {
                    MultiTenantContext.setTenant(tenant);
                    List<ColumnMetadata> cms = getDecoratedMetadata(customerSpace, entity, version, groups)
                            .collectList().block();
                    if (CollectionUtils.isNotEmpty(cms)
                            && BusinessEntity.ENTITIES_WITH_HIRERARCHICAL_DISPLAY_NAME.contains(entity)) {
                        cms.forEach(cm -> {
                            String subCategory = cm.getSubcategory();
                            if (StringUtils.isNotBlank(subCategory) && !"Others".equalsIgnoreCase(subCategory)) {
                                String displayName = cm.getDisplayName();
                                cm.setDisplayName(subCategory + ": " + displayName);
                            }
                        });
                    }
                    return CollectionUtils.isNotEmpty(cms) ? cms : new ArrayList<>();
                }));
        map.values().forEach(columnMetadataList::addAll);
        return columnMetadataList;
    }

    @Override
    public Flux<ColumnMetadata> getAttrsCanBeEnabledForModeling(String customerSpace, BusinessEntity entity,
            DataCollection.Version version, Boolean allCustomerAttrs) {
        if (!BusinessEntity.MODELING_ENTITIES.contains(entity)) {
            throw new UnsupportedOperationException(String.format("%s is not supported for modeling.", entity));
        }
        Flux<ColumnMetadata> flux = getSystemMetadataAttrFlux(customerSpace, entity, version);
        flux = flux.map(cm -> {
            if (cm.getTagList() == null || (cm.getTagList() != null && !cm.getTagList().contains(Tag.EXTERNAL))) {
                cm.setTagList(Collections.singletonList(Tag.INTERNAL));
            }
            return cm;
        });
        if (Boolean.TRUE.equals(allCustomerAttrs)) {
            flux = flux.filter(cm -> // not external (not LDC) or can model
            cm.getTagList().contains(Tag.INTERNAL) || Boolean.TRUE.equals(cm.getCanModel()));
        } else {
            flux = flux.filter(cm -> Boolean.TRUE.equals(cm.getCanModel()));
        }
        return flux;
    }

    @Override
    public Flux<ColumnMetadata> getSystemMetadataAttrFlux(String customerSpace, BusinessEntity entity,
            DataCollection.Version version) {
        AtomicLong timer = new AtomicLong();
        AtomicLong counter = new AtomicLong();
        Flux<ColumnMetadata> flux;
        flux = getSystemMetadata(entity,
                version != null ? version : dataCollectionService.getActiveVersion(customerSpace)).sequential();
        flux = flux //
                .doOnSubscribe(s -> {
                    timer.set(System.currentTimeMillis());
                    log.info("Start serving system metadata for " + customerSpace + ":" + customerSpace);
                }) //
                .doOnNext(cm -> counter.getAndIncrement()) //
                .doOnComplete(() -> {
                    long duration = System.currentTimeMillis() - timer.get();
                    log.info("Finished serving system metadata for " + counter.get() + " attributes from "
                            + customerSpace + " TimeElapsed=" + duration + " msec");
                });
        return flux;
    }

    private ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(String tenantId, BusinessEntity entity,
                                                                   DataCollection.Version version, StoreFilter filter) {
        return getFullyDecoratedMetadata(tenantId, entity, version, filter, null);
    }

    private ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(String tenantId, BusinessEntity entity,
            DataCollection.Version version, StoreFilter filter, String attributeSetName) {
        String customerSpace = CustomerSpace.parse(tenantId).toString();
        TableRoleInCollection role = entity.getServingStore();
        List<String> tables = dataCollectionService.getTableNames(customerSpace, "", role, version);
        if (CollectionUtils.isEmpty(tables) && BusinessEntity.Account.equals(entity)) {
            role = BusinessEntity.Account.getBatchStore();
            tables = dataCollectionService.getTableNames(customerSpace, "", role, version);
        }
        ParallelFlux<ColumnMetadata> flux;
        if (CollectionUtils.isNotEmpty(tables)) {
            flux = customizedMetadataStore.getMetadataInParallel(entity, version, filter, attributeSetName).map(cm -> {
                cm.setBitOffset(null);
                cm.setNumBits(null);
                cm.setPhysicalName(null);
                cm.setStatisticalType(null);
                cm.setStats(null);
                cm.setDecodeStrategy(null);

                if (Boolean.TRUE.equals(cm.getShouldDeprecate()) && !AttrState.Inactive.equals(cm.getAttrState())) {
                    cm.setAttrState(AttrState.Deprecated);
                }

                if (AttrState.Inactive.equals(cm.getAttrState())) {
                    // disable these usages if it is inactive attribute.
                    cm.disableGroup(ColumnSelection.Predefined.Segment);
                    cm.disableGroup(ColumnSelection.Predefined.Enrichment);
                    cm.disableGroup(ColumnSelection.Predefined.TalkingPoint);
                    cm.disableGroup(ColumnSelection.Predefined.CompanyProfile);
                }

                if (AttrState.Deprecated.equals(cm.getAttrState())) {
                    // disable these usages if it is deprecated attribute.
                    cm.disableGroup(ColumnSelection.Predefined.Enrichment);
                    cm.disableGroup(ColumnSelection.Predefined.CompanyProfile);
                }

                return cm;
            });
        } else {
            flux = Flux.<ColumnMetadata> empty().parallel();
        }
        return flux;
    }

    @Override
    public Flux<ColumnMetadata> getAttrsEnabledForModeling(String customerSpace, BusinessEntity entity,
            DataCollection.Version version) {
        Flux<ColumnMetadata> flux = getDecoratedMetadata(customerSpace, entity, version,
                Collections.singletonList(ColumnSelection.Predefined.Model));
        flux = flux.map(cm -> {
            cm.setApprovedUsageList(Collections.singletonList(ApprovedUsage.MODEL_ALLINSIGHTS));
            if (cm.getTagList() == null || (cm.getTagList() != null && !cm.getTagList().contains(Tag.EXTERNAL))) {
                cm.setTagList(Collections.singletonList(Tag.INTERNAL));
            }
            return cm;
        });
        return flux;
    }
}
