package com.latticeengines.apps.cdl.mds.impl;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.RawSystemMetadataStore;
import com.latticeengines.apps.cdl.mds.TableRoleTemplate;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.core.mds.AMMetadataStore;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.CategoryUtils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

/**
 * Metadata directly read from Tables
 */
@Component("rawSystemMetadataStore")
public class RawSystemMetadataStoreImpl implements RawSystemMetadataStore {

    private static final Logger log = LoggerFactory.getLogger(RawSystemMetadataStoreImpl.class);

    private final DataCollectionService dataCollectionService;
    private final TableRoleTemplate tableRoleTemplate;
    private final AMMetadataStore amMetadataStore;
    private final CDLNamespaceService cdlNamespaceService;
    private final BatonService batonService;

    @Inject
    RawSystemMetadataStoreImpl(DataCollectionService dataCollectionService, //
            TableRoleTemplate tableRoleTemplate, //
            AMMetadataStore amMetadataStore, //
            CDLNamespaceService cdlNamespaceService, //
            BatonService batonService) {
        this.dataCollectionService = dataCollectionService;
        this.tableRoleTemplate = tableRoleTemplate;
        this.amMetadataStore = amMetadataStore;
        this.cdlNamespaceService = cdlNamespaceService;
        this.batonService = batonService;
    }

    @Override
    public Flux<ColumnMetadata> getMetadata(Namespace2<BusinessEntity, DataCollection.Version> namespace) {
        return getMetadataInParallel(namespace).sequential();
    }

    @Override
    public ParallelFlux<ColumnMetadata> getMetadataInParallel(
            Namespace2<BusinessEntity, DataCollection.Version> namespace) {
        BusinessEntity entity = namespace.getCoord1();
        boolean usingAccountServingStore = false;
        TableRoleInCollection role = entity.getServingStore();
        if (BusinessEntity.Account.equals(entity)) {
            // only account uses batch store
            role = BusinessEntity.Account.getBatchStore();
        }

        DataCollection.Version version = namespace.getCoord2();
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        boolean entityMatchEnabled = batonService.isEnabled(MultiTenantContext.getCustomerSpace(),
                LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        List<String> tableNames = dataCollectionService.getTableNames(customerSpace, "", role, version);
        if (BusinessEntity.Account.equals(entity) && CollectionUtils.isEmpty(tableNames)) {
            // for account try serving store
            role = BusinessEntity.Account.getServingStore();
            tableNames = dataCollectionService.getTableNames(customerSpace, "", role, version);
            usingAccountServingStore = true;
            log.info("Cannot find Account batch store, using Account serving store instead.");
        }

        ParallelFlux<ColumnMetadata> servingStore;
        if (CollectionUtils.isNotEmpty(tableNames)) {
            Category category = CategoryUtils.getEntityCategory(entity);
            Namespace2<TableRoleInCollection, DataCollection.Version> trNs = Namespace.as(role, version);
            ThreadLocal<AtomicLong> counter = new ThreadLocal<>();
            servingStore = tableRoleTemplate.getUnorderedSchema(trNs);
            if (usingAccountServingStore) {
                servingStore = servingStore.filter(cm -> Category.ACCOUNT_ATTRIBUTES.equals(cm.getCategory()));
            }

            Set<String> systemAttributes = SchemaRepository.getSystemAttributes(entity, entityMatchEnabled).stream()
                    .map(InterfaceName::name).collect(Collectors.toSet());

            servingStore = servingStore //
                    .map(cm -> {
                        cm.setEntity(entity);
                        cm.setCategory(category);
                        if (systemAttributes.contains(cm.getAttrName())) {
                            cm.setGroups(null);
                        }
                        return cm;
                    }) //
                    .doOnNext(cm -> {
                        if (counter.get() == null) {
                            counter.set(new AtomicLong(0));
                        }
                        counter.get().getAndIncrement();
                    }) //
                    .doOnComplete(() -> {
                        long count = 0;
                        if (counter.get() != null) {
                            count = counter.get().get();
                        }
                        log.info("Retrieved " + count + " " + entity + " attributes.");
                    });
        } else {
            log.info("There is not table for " + role + " at version " + version + " in  " + customerSpace
                    + ", using empty schema");
            servingStore = Flux.<ColumnMetadata> empty().parallel().runOn(scheduler);
        }
        ThreadLocal<AtomicLong> amCounter = new ThreadLocal<>();
        if (BusinessEntity.Account.equals(entity)) {
            // merge serving store and AM, for Account
            Namespace1<String> amNs = cdlNamespaceService.resolveDataCloudVersion(version);
            ParallelFlux<ColumnMetadata> amFlux = amMetadataStore.getMetadataInParallel(amNs) //
                    .filter(cm -> !InterfaceName.LatticeAccountId.name().equals(cm.getAttrName())) //
                    .map(cm -> {
                        cm.setEntity(BusinessEntity.Account);
                        return cm;
                    }).doOnNext(cm -> {
                        if (amCounter.get() == null) {
                            amCounter.set(new AtomicLong(0));
                        }
                        amCounter.get().getAndIncrement();
                    }) //
                    .doOnComplete(() -> {
                        long count = 0;
                        if (amCounter.get() != null) {
                            count = amCounter.get().get();
                        }
                        log.info("Inserted " + count + " AM attributes.");
                    });
            return ParallelFlux.from(servingStore, amFlux);
        } else {
            return servingStore;
        }
    }

    @Override
    public String getName() {
        return "raw-system-mds";
    }
}
