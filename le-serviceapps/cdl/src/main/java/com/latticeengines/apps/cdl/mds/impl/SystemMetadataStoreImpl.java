package com.latticeengines.apps.cdl.mds.impl;

import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Enrichment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Model;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Segment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.TalkingPoint;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.ExternalSystemMetadataStore;
import com.latticeengines.apps.cdl.mds.RatingDisplayMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.mds.TableRoleTemplate;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.core.mds.AMMetadataStore;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.mds.ChainedDecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.DecoratedMetadataStore;
import com.latticeengines.domain.exposed.metadata.mds.DecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.MdsDecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.CategoryUtils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@Component
public class SystemMetadataStoreImpl extends
        DecoratedMetadataStore<Namespace2<BusinessEntity, DataCollection.Version>, Namespace2<BusinessEntity, DataCollection.Version>, Namespace1<BusinessEntity>>
        implements SystemMetadataStore {

    private static final Logger log = LoggerFactory.getLogger(SystemMetadataStoreImpl.class);

    @Inject
    public SystemMetadataStoreImpl(DataCollectionService dataCollectionService, TableRoleTemplate tableRoleTemplate,
            AMMetadataStore amMetadataStore, CDLNamespaceService cdlNamespaceService,
            RatingDisplayMetadataStore ratingDisplayMetadataStore,
            ExternalSystemMetadataStore externalSystemMetadataStore) {
        super(getBaseMds(dataCollectionService, tableRoleTemplate, amMetadataStore, cdlNamespaceService),
                getDecoratorChain(ratingDisplayMetadataStore, externalSystemMetadataStore));
    }

    private static MetadataStore<Namespace2<BusinessEntity, DataCollection.Version>> getBaseMds(
            DataCollectionService dataCollectionService, TableRoleTemplate tableRoleTemplate,
            AMMetadataStore amMetadataStore, CDLNamespaceService cdlNamespaceService) {

        return new MetadataStore<Namespace2<BusinessEntity, DataCollection.Version>>() {

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

                    Set<String> internalAttributes = SchemaRepository.getSystemAttributes(entity).stream()
                            .map(InterfaceName::name).collect(Collectors.toSet());

                    Set<String> exportAttributes = SchemaRepository.getDefaultExportAttributes(entity).stream()
                            .map(InterfaceName::name).collect(Collectors.toSet());

                    servingStore = servingStore //
                            .map(cm -> {
                                cm.setEntity(entity);
                                cm.setCategory(category);

                                if (internalAttributes.contains(cm.getAttrName())) {
                                    cm.setGroups(null);
                                } else {
                                    // all non-LDC attributes can be
                                    // enabled/disabled for Segmentation
                                    cm.setCanSegment(true);

                                    // all non-LDC attributes are enabled for Segmentation
                                    // unless otherwise specified in upstream
                                    cm.enableGroupIfNotPresent(Segment);

                                    // all custom account and contact attributes
                                    // can be enabled/disabled for Export
                                    if (BusinessEntity.Account.equals(entity)
                                            || BusinessEntity.Contact.equals(entity)) {
                                        cm.setCanEnrich(true);
                                    }

                                    // all custom account attributes enabled for following groups
                                    // unless otherwise specified in upstream
                                    if (BusinessEntity.Account.equals(entity)) {
                                        cm.enableGroupIfNotPresent(Model);
                                        cm.enableGroupIfNotPresent(TalkingPoint);
                                    }
                                }

                                // enable a list of default attributes for Export
                                if (exportAttributes.contains(cm.getAttrName())) {
                                    cm.setCanEnrich(true);
                                    cm.enableGroupIfNotPresent(Enrichment);
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
                    Namespace1<String> amNs = cdlNamespaceService.resolveDataCloudVersion();
                    ParallelFlux<ColumnMetadata> amFlux = amMetadataStore.getMetadataInParallel(amNs) //
                            .filter(cm -> !InterfaceName.LatticeAccountId.name().equals(cm.getAttrName())) //
                            .map(cm -> {
                                cm.setEntity(BusinessEntity.Account);

                                if (Category.FIRMOGRAPHICS.equals(cm.getCategory())
                                        || StringUtils.isNotBlank(cm.getDataLicense())) {
                                    cm.enableGroup(Enrichment);
                                } else {
                                    cm.disableGroup(Enrichment);
                                }

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
        };
    }

    private static ChainedDecoratorFactory<Namespace1<BusinessEntity>> getDecoratorChain(
            RatingDisplayMetadataStore ratingDisplayMetadataStore,
            ExternalSystemMetadataStore externalSystemMetadataStore) {
        DecoratorFactory<Namespace1<String>> ratingDisplayDecorator = getRatingDecorator(ratingDisplayMetadataStore);
        DecoratorFactory<Namespace2<String, BusinessEntity>> lookupIdDecorator = getLookupIdDecorator(
                externalSystemMetadataStore);
        List<DecoratorFactory<? extends Namespace>> factories = Arrays.asList( //
                lookupIdDecorator, //
                ratingDisplayDecorator //

        );

        return new ChainedDecoratorFactory<Namespace1<BusinessEntity>>("ServingStoreChain", factories) {
            @Override
            protected List<Namespace> project(Namespace1<BusinessEntity> namespace) {
                BusinessEntity entity = namespace.getCoord1();
                String tenantId = MultiTenantContext.getTenantId();
                Namespace ratingNs = Namespace.as(BusinessEntity.Rating.equals(entity) ? tenantId : "");
                Namespace lookupIdNs = Namespace.as(tenantId, entity);
                return Arrays.asList(lookupIdNs, ratingNs);
            }
        };
    }

    private static DecoratorFactory<Namespace1<String>> getRatingDecorator(
            RatingDisplayMetadataStore ratingDisplayMetadataStore) {
        return MdsDecoratorFactory.fromMds("RatingDisplay", ratingDisplayMetadataStore);
    }

    private static DecoratorFactory<Namespace2<String, BusinessEntity>> getLookupIdDecorator(
            ExternalSystemMetadataStore externalSystemMetadataStore) {
        return MdsDecoratorFactory.fromMds("LookupId", externalSystemMetadataStore);
    }

    @Override
    protected Namespace2<BusinessEntity, DataCollection.Version> projectBaseNamespace(
            Namespace2<BusinessEntity, DataCollection.Version> namespace) {
        return namespace;
    }

    @Override
    protected Namespace1<BusinessEntity> projectDecoratorNamespace(
            Namespace2<BusinessEntity, DataCollection.Version> namespace) {
        return Namespace.as(namespace.getCoord1());
    }

}
