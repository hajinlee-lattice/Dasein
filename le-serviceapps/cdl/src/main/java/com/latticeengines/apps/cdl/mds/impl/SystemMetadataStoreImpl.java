package com.latticeengines.apps.cdl.mds.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.AccountAttrsDecoratorFac;
import com.latticeengines.apps.cdl.mds.ActivityMetricDecoratorFac;
import com.latticeengines.apps.cdl.mds.ContactAttrsDecoratorFac;
import com.latticeengines.apps.cdl.mds.CuratedAttrsMetadataStore;
import com.latticeengines.apps.cdl.mds.CuratedContactAttrsMetadataStore;
import com.latticeengines.apps.cdl.mds.ExternalSystemMetadataStore;
import com.latticeengines.apps.cdl.mds.ImportSystemAttrsDecoratorFac;
import com.latticeengines.apps.cdl.mds.RatingDisplayMetadataStore;
import com.latticeengines.apps.cdl.mds.RawSystemMetadataStore;
import com.latticeengines.apps.cdl.mds.SpendAnalyticMetricsDecoratorFac;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.mds.ChainedDecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.DecoratedMetadataStore;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.MdsDecoratorFactory;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace0;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace3;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;

@Component("systemMetadataStore")
public class SystemMetadataStoreImpl extends
        DecoratedMetadataStore<//
                Namespace3<BusinessEntity, DataCollection.Version, StoreFilter>, //
                Namespace3<BusinessEntity, DataCollection.Version, StoreFilter>, //
                Namespace2<BusinessEntity, DataCollection.Version>>
        implements SystemMetadataStore {

    @Inject
    public SystemMetadataStoreImpl( //
                                    RawSystemMetadataStore rawSystemMetadataStore, //
                                    AccountAttrsDecoratorFac accountAttrsDecorator, //
                                    ContactAttrsDecoratorFac contactAttrsDecorator, //
                                    ImportSystemAttrsDecoratorFac importSystemAttrsDecorator, //
                                    SpendAnalyticMetricsDecoratorFac activityMetricsDecorator, //
                                    RatingDisplayMetadataStore ratingDisplayMetadataStore, //
                                    ExternalSystemMetadataStore externalSystemMetadataStore, //
                                    CuratedAttrsMetadataStore derivedAttrsMetadataStore, //
                                    CuratedContactAttrsMetadataStore curatedContactAttrsMetadataStore, //
                                    ActivityMetricDecoratorFac activityMetricDecorator) {
        super(rawSystemMetadataStore, //
                getDecoratorChain(//
                        accountAttrsDecorator, //
                        contactAttrsDecorator, //
                        importSystemAttrsDecorator, //
                        activityMetricsDecorator, //
                        ratingDisplayMetadataStore, //
                        externalSystemMetadataStore, //
                        derivedAttrsMetadataStore, //
                        curatedContactAttrsMetadataStore, //
                        activityMetricDecorator //
                ));
    }

    private static ChainedDecoratorFactory<Namespace2<BusinessEntity, DataCollection.Version>> getDecoratorChain(
            AccountAttrsDecoratorFac accountAttrsDecorator, //
            ContactAttrsDecoratorFac contactAttrsDecorator, //
            ImportSystemAttrsDecoratorFac importSystemAttrsDecorator, //
            SpendAnalyticMetricsDecoratorFac spendAnalyticMetricsDecorator, //
            RatingDisplayMetadataStore ratingDisplayMetadataStore, //
            ExternalSystemMetadataStore externalSystemMetadataStore, //
            CuratedAttrsMetadataStore curatedAttrsMetadataStore, //
            CuratedContactAttrsMetadataStore curatedContactAttrsMetadataStore, //
            ActivityMetricDecoratorFac activityMetricDecorator) {
        DecoratorFactory<Namespace1<String>> ratingDisplayDecorator = //
                MdsDecoratorFactory.fromMds("RatingDisplay", ratingDisplayMetadataStore);
        DecoratorFactory<Namespace2<String, BusinessEntity>> lookupIdDecorator = //
                MdsDecoratorFactory.fromMds("LookupId", externalSystemMetadataStore);
        DecoratorFactory<Namespace2<String, DataCollection.Version>> curatedAttrsDecorator = //
                MdsDecoratorFactory.fromMds("CuratedAttrs", curatedAttrsMetadataStore);
        DecoratorFactory<Namespace2<String, DataCollection.Version>> curatedContactAttrsDecorator = //
                MdsDecoratorFactory.fromMds("CuratedContactAttrs", curatedContactAttrsMetadataStore);
        Decorator apsAttrDecorator = new APSAttrsDecorator();
        Decorator postRenderDecorator = new SystemPostRenderDecorator();
        // order in sync with ChainedDecoratorFactory.project() below
        List<DecoratorFactory<? extends Namespace>> factories = Arrays.asList(//
                accountAttrsDecorator, //
                contactAttrsDecorator, //
                lookupIdDecorator, //
                importSystemAttrsDecorator, //
                spendAnalyticMetricsDecorator, //
                apsAttrDecorator, //
                ratingDisplayDecorator, //
                curatedAttrsDecorator, //
                curatedContactAttrsDecorator, //
                activityMetricDecorator, //
                postRenderDecorator //
        );

        return new ChainedDecoratorFactory<Namespace2<BusinessEntity, DataCollection.Version>>("ServingStoreChain",
                factories) {
            @Override
            protected List<Namespace> project(Namespace2<BusinessEntity, DataCollection.Version> namespace) {
                BusinessEntity entity = namespace.getCoord1();
                String tenantId = MultiTenantContext.getShortTenantId();
                Namespace accountNs = Namespace.as(BusinessEntity.Account.equals(entity) ? tenantId : "");
                Namespace contactNs = Namespace.as(BusinessEntity.Contact.equals(entity) ? tenantId : "");
                Namespace importSystemNs =
                        Namespace.as((BusinessEntity.Account.equals(entity) || BusinessEntity.Contact.equals(entity)) ? tenantId : "");
                Namespace phNs = Namespace
                        .as(BusinessEntity.PurchaseHistory.equals(entity) ? tenantId : "");
                Namespace ratingNs = Namespace.as(BusinessEntity.Rating.equals(entity) ? tenantId : "");
                Namespace curatedNs = Namespace.as(//
                        BusinessEntity.CuratedAccount.equals(entity) ? tenantId : "", //
                        namespace.getCoord2());
                Namespace curatedContactNs = Namespace.as(//
                        BusinessEntity.CuratedContact.equals(entity) ? tenantId : "", //
                        namespace.getCoord2());
                Namespace activityMetricNs = Namespace.as(BusinessEntity.ACTIVITY_METRIC_SERVING_ENTITIES.contains(entity) //
                        ? tenantId : "");
                Namespace lookupIdNs = Namespace.as(tenantId, entity);
                // order in sync with getDecoratorChain()
                return Arrays.asList( //
                        accountNs, //
                        contactNs, //
                        lookupIdNs, //
                        importSystemNs, //
                        phNs, //
                        Namespace0.NS, //
                        ratingNs, //
                        curatedNs, //
                        curatedContactNs, //
                        activityMetricNs, //
                        Namespace0.NS //
                );
            }
        };

    }

    @Override
    protected Namespace2<BusinessEntity, DataCollection.Version> projectDecoratorNamespace(
            Namespace3<BusinessEntity, DataCollection.Version, StoreFilter> namespace) {
        return namespace;
    }

    @Override
    protected Namespace3<BusinessEntity, DataCollection.Version, StoreFilter> projectBaseNamespace(
            Namespace3<BusinessEntity, DataCollection.Version, StoreFilter> namespace) {
        return namespace;
    }

}
