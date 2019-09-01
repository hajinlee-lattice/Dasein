package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
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
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

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
        return systemMetadataStore.getMetadataInParallel(entity, version);
    }

    @Override
    public ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity,
            DataCollection.Version version) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return getFullyDecoratedMetadata(customerSpace, entity, version);
    }

    @Override
    public Flux<ColumnMetadata> getFullyDecoratedMetadataInOrder(BusinessEntity entity,
            DataCollection.Version version) {
        return getFullyDecoratedMetadata(entity, version).sorted(Comparator.comparing(ColumnMetadata::getAttrName));
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.ServingMetadataCacheName, key = "T(java.lang.String).format(\"%s|%s|md_in_svc\", #tenantId, #entity)", unless="#result == null")
    public List<ColumnMetadata> getDecoratedMetadataFromCache(String tenantId, BusinessEntity entity) {
        String customerSpace = CustomerSpace.parse(tenantId).toString();
        DataCollection.Version version = dataCollectionService.getActiveVersion(customerSpace);
        return getFullyDecoratedMetadata(customerSpace, entity, version).sequential().collectList().block();
    }

    @Override
    public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity, DataCollection.Version version, List<ColumnSelection.Predefined> groups) {
        AtomicLong timer = new AtomicLong();
        AtomicLong counter = new AtomicLong();
        Flux<ColumnMetadata> flux;
        if (version == null) {
            flux =
                    getFullyDecoratedMetadata(entity, dataCollectionService.getActiveVersion(customerSpace))
                            .sequential();
        } else {
            flux = getFullyDecoratedMetadata(entity, version).sequential();
        }
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
        return flux;
    }

    @Override
    public Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace, BusinessEntity entity, DataCollection.Version version, Boolean allCustomerAttrs) {
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

    public ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(String tenantId, BusinessEntity entity,
                                                                  DataCollection.Version version) {
        String customerSpace = CustomerSpace.parse(tenantId).toString();
        TableRoleInCollection role = entity.getServingStore();
        List<String> tables = dataCollectionService.getTableNames(customerSpace, "", role, version);
        if (CollectionUtils.isEmpty(tables) && BusinessEntity.Account.equals(entity)) {
            role = BusinessEntity.Account.getBatchStore();
            tables = dataCollectionService.getTableNames(customerSpace, "", role, version);
        }
        ParallelFlux<ColumnMetadata> flux;
        if (CollectionUtils.isNotEmpty(tables)) {
            flux = customizedMetadataStore.getMetadataInParallel(entity, version).map(cm -> {
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
                    // disable these useages if it is inactive attribute.
                    cm.disableGroup(ColumnSelection.Predefined.Segment);
                    cm.disableGroup(ColumnSelection.Predefined.Enrichment);
                    cm.disableGroup(ColumnSelection.Predefined.TalkingPoint);
                    cm.disableGroup(ColumnSelection.Predefined.CompanyProfile);
                }

                if (AttrState.Deprecated.equals(cm.getAttrState())) {
                    // disable these useages if it is deprecated attribute.
                    cm.disableGroup(ColumnSelection.Predefined.Enrichment);
                    cm.disableGroup(ColumnSelection.Predefined.CompanyProfile);
                }

                return cm;
            });
        } else {
            flux = Flux.<ColumnMetadata>empty().parallel();
        }
        return flux;
    }

    @Override
    public Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace, BusinessEntity entity, DataCollection.Version version) {
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
