package com.latticeengines.apps.cdl.mds.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.RatingDisplayMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.mds.TableRoleTemplate;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
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
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@Component
public class SystemMetadataStoreImpl extends
        DecoratedMetadataStore<Namespace2<BusinessEntity, DataCollection.Version>, Namespace2<BusinessEntity, DataCollection.Version>, Namespace1<BusinessEntity>>
        implements SystemMetadataStore {

    private static final Logger log = LoggerFactory.getLogger(SystemMetadataStoreImpl.class);

    @Inject
    public SystemMetadataStoreImpl(TableRoleTemplate tableRoleTemplate, AMMetadataStore amMetadataStore,
            CDLNamespaceService cdlNamespaceService, RatingDisplayMetadataStore ratingDisplayMetadataStore) {
        super(getBaseMds(tableRoleTemplate, amMetadataStore, cdlNamespaceService),
                getDecoratorChain(ratingDisplayMetadataStore));
    }

    private static MetadataStore<Namespace2<BusinessEntity, DataCollection.Version>> getBaseMds(
            TableRoleTemplate tableRoleTemplate, AMMetadataStore amMetadataStore,
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
                Category category = getEntityCategory(entity);
                Namespace2<TableRoleInCollection, DataCollection.Version> trNs = Namespace.as(entity.getServingStore(),
                        namespace.getCoord2());
                ThreadLocal<AtomicLong> counter = new ThreadLocal<>();
                ParallelFlux<ColumnMetadata> servingStore = tableRoleTemplate.getUnorderedSchema(trNs) //
                        .map(cm -> {
                            if (cm.getCategory() == null) {
                                cm.setCategory(category);
                            }
                            cm.setEntity(entity);
                            cm.enableGroup(ColumnSelection.Predefined.Segment);
                            if (BusinessEntity.Account.equals(entity)) {
                                cm.enableGroup(ColumnSelection.Predefined.TalkingPoint);
                                cm.enableGroup(ColumnSelection.Predefined.CompanyProfile);
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
                ThreadLocal<AtomicLong> amCounter = new ThreadLocal<>();
                if (BusinessEntity.Account.equals(entity)) {
                    // merge serving store and AM, for Account
                    Namespace1<String> amNs = cdlNamespaceService.resolveDataCloudVersion();
                    ParallelFlux<ColumnMetadata> amFlux = amMetadataStore.getMetadataInParallel(amNs) //
                            .filter(cm -> !InterfaceName.LatticeAccountId.name().equals(cm.getAttrName())) //
                            .filter(cm -> !cm.isEnabledFor(ColumnSelection.Predefined.Segment)) //
                            .doOnNext(cm -> {
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
            RatingDisplayMetadataStore ratingDisplayMetadataStore) {
        DecoratorFactory<Namespace1<String>> ratingDisplayDecorator = getRatingDecorator(ratingDisplayMetadataStore);
        List<DecoratorFactory<? extends Namespace>> factories = Collections.singletonList( //
                ratingDisplayDecorator //
        );

        return new ChainedDecoratorFactory<Namespace1<BusinessEntity>>("ServingStoreChain", factories) {
            @Override
            protected List<Namespace> project(Namespace1<BusinessEntity> namespace) {
                BusinessEntity entity = namespace.getCoord1();
                String tenantId = MultiTenantContext.getTenantId();
                Namespace ratingNs = Namespace.as(BusinessEntity.Rating.equals(entity) ? tenantId : "");
                return Collections.singletonList(ratingNs);
            }
        };
    }

    private static DecoratorFactory<Namespace1<String>> getRatingDecorator(
            RatingDisplayMetadataStore ratingDisplayMetadataStore) {
        return MdsDecoratorFactory.fromMds("RatingDisplay", ratingDisplayMetadataStore);
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

    private static Category getEntityCategory(BusinessEntity entity) {
        Category category;
        switch (entity) {
        case Account:
            category = Category.ACCOUNT_ATTRIBUTES;
            break;
        case Contact:
            category = Category.CONTACT_ATTRIBUTES;
            break;
        case PurchaseHistory:
            category = Category.PRODUCT_SPEND;
            break;
        case Rating:
            category = Category.RATING;
            break;
        default:
            category = Category.DEFAULT;
        }
        return category;
    }

}
