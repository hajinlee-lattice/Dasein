package com.latticeengines.apps.cdl.mds.impl;

import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.CompanyProfile;
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

import com.latticeengines.apps.cdl.mds.DerivedAttrsMetadataStore;
import com.latticeengines.apps.cdl.mds.ExternalSystemMetadataStore;
import com.latticeengines.apps.cdl.mds.RatingDisplayMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.mds.TableRoleTemplate;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.core.mds.AMMetadataStore;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.mds.ChainedDecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.DecoratedMetadataStore;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.MdsDecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace0;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.util.CategoryUtils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@Component
public class SystemMetadataStoreImpl extends DecoratedMetadataStore<//
        Namespace2<BusinessEntity, DataCollection.Version>, //
        Namespace2<BusinessEntity, DataCollection.Version>, //
        Namespace2<BusinessEntity, DataCollection.Version>> implements SystemMetadataStore {

    private static final Logger log = LoggerFactory.getLogger(SystemMetadataStoreImpl.class);

    @Inject
    public SystemMetadataStoreImpl(DataCollectionService dataCollectionService, ZKConfigService zkConfigService,
            CDLNamespaceService cdlNamespaceService, //
            TableRoleTemplate tableRoleTemplate, //
            AMMetadataStore amMetadataStore, //
            RatingDisplayMetadataStore ratingDisplayMetadataStore, //
            ExternalSystemMetadataStore externalSystemMetadataStore, //
            DerivedAttrsMetadataStore derivedAttrsMetadataStore) {
        super(getBaseMds(//
                dataCollectionService, zkConfigService, //
                tableRoleTemplate, //
                amMetadataStore, //
                cdlNamespaceService //
        ), getDecoratorChain(//
                ratingDisplayMetadataStore, //
                externalSystemMetadataStore, //
                derivedAttrsMetadataStore //
        ));
    }

    private static MetadataStore<Namespace2<BusinessEntity, DataCollection.Version>> getBaseMds(
            DataCollectionService dataCollectionService, ZKConfigService zkConfigService, //
            TableRoleTemplate tableRoleTemplate, //
            AMMetadataStore amMetadataStore, //
            CDLNamespaceService cdlNamespaceService) {

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

                    Set<String> systemAttributes = SchemaRepository.getSystemAttributes(entity).stream()
                            .map(InterfaceName::name).collect(Collectors.toSet());

                    Set<String> exportAttributes = SchemaRepository.getDefaultExportAttributes(entity).stream()
                            .map(InterfaceName::name).collect(Collectors.toSet());

                    servingStore = servingStore //
                            .map(cm -> {
                                cm.setEntity(entity);
                                cm.setCategory(category);

                                if (systemAttributes.contains(cm.getAttrName())) {
                                    cm.setGroups(null);
                                    return cm;
                                }

                                // Segmentation
                                cm.setCanSegment(true);
                                cm.enableGroup(Segment);

                                // Enrichment
                                cm.setCanEnrich(true);

                                // enable a list of default attributes for
                                // Export
                                if (exportAttributes.contains(cm.getAttrName())) {
                                    cm.enableGroup(Enrichment);
                                } else {
                                    cm.disableGroup(Enrichment);
                                }
                                cm.enableGroup(TalkingPoint);
                                cm.disableGroup(CompanyProfile);
                                cm.disableGroup(Model);
                                cm.setCanModel(false);

                                if (BusinessEntity.Account.equals(entity)) {
                                    if (InterfaceName.AccountId.name().equalsIgnoreCase(cm.getAttrName())) {
                                        cm.setSubcategory("Account IDs");
                                    }
                                    cm.setCanModel(true);
                                }

                                // TODO: YSong (M22) to be moved to a specific
                                // metadata store for curated attrs
                                if (BusinessEntity.PurchaseHistory.equals(entity)) {
                                    cm.disableGroup(Enrichment);
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
                    boolean internalEnrichEnabled = zkConfigService
                            .isInternalEnrichmentEnabled(CustomerSpace.parse(customerSpace));
                    ParallelFlux<ColumnMetadata> amFlux = amMetadataStore.getMetadataInParallel(amNs) //
                            .filter(cm -> !InterfaceName.LatticeAccountId.name().equals(cm.getAttrName())) //
                            .map(cm -> {
                                cm.setEntity(BusinessEntity.Account);

                                // Initial status for Export: enabled for
                                // Firmographics and premium
                                if (!internalEnrichEnabled && Boolean.TRUE.equals(cm.getCanInternalEnrich())) {
                                    cm.disableGroup(Enrichment);
                                    cm.disableGroup(TalkingPoint);
                                    cm.setCanEnrich(false);
                                    cm.setAttrState(AttrState.Inactive);
                                } else if (Boolean.TRUE.equals(cm.getCanEnrich())
                                        && (Category.FIRMOGRAPHICS.equals(cm.getCategory())
                                                || StringUtils.isNotBlank(cm.getDataLicense()))) {
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

    private static ChainedDecoratorFactory<Namespace2<BusinessEntity, DataCollection.Version>> getDecoratorChain(
            RatingDisplayMetadataStore ratingDisplayMetadataStore,
            ExternalSystemMetadataStore externalSystemMetadataStore,
            DerivedAttrsMetadataStore derivedAttrsMetadataStore) {
        DecoratorFactory<Namespace1<String>> ratingDisplayDecorator = //
                MdsDecoratorFactory.fromMds("RatingDisplay", ratingDisplayMetadataStore);
        DecoratorFactory<Namespace2<String, BusinessEntity>> lookupIdDecorator = //
                MdsDecoratorFactory.fromMds("LookupId", externalSystemMetadataStore);
        DecoratorFactory<Namespace2<String, DataCollection.Version>> derivedAttrsDecorator = //
                MdsDecoratorFactory.fromMds("DerivedAttrs", derivedAttrsMetadataStore);
        Decorator postRenderDecorator = new SystemPostRenderDecorator();
        List<DecoratorFactory<? extends Namespace>> factories = Arrays.asList(//
                lookupIdDecorator, //
                ratingDisplayDecorator, //
                derivedAttrsDecorator, //
                postRenderDecorator

        );

        return new ChainedDecoratorFactory<Namespace2<BusinessEntity, DataCollection.Version>>("ServingStoreChain",
                factories) {
            @Override
            protected List<Namespace> project(Namespace2<BusinessEntity, DataCollection.Version> namespace) {
                BusinessEntity entity = namespace.getCoord1();
                String tenantId = MultiTenantContext.getShortTenantId();
                Namespace ratingNs = Namespace.as(BusinessEntity.Rating.equals(entity) ? tenantId : "");
                Namespace derivedNs = Namespace.as(//
                        BusinessEntity.CuratedAccount.equals(entity) ? tenantId : "", //
                        namespace.getCoord2());
                Namespace lookupIdNs = Namespace.as(tenantId, entity);
                return Arrays.asList(lookupIdNs, ratingNs, derivedNs, Namespace0.NS);
            }
        };
    }

    @Override
    protected Namespace2<BusinessEntity, DataCollection.Version> projectBaseNamespace(
            Namespace2<BusinessEntity, DataCollection.Version> namespace) {
        return namespace;
    }

    @Override
    protected Namespace2<BusinessEntity, DataCollection.Version> projectDecoratorNamespace(
            Namespace2<BusinessEntity, DataCollection.Version> namespace) {
        return namespace;
    }

}
