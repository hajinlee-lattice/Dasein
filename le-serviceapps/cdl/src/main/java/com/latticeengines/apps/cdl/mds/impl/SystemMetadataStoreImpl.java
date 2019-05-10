package com.latticeengines.apps.cdl.mds.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.ActivityMetricsDecoratorFac;
import com.latticeengines.apps.cdl.mds.CuratedAttrsMetadataStore;
import com.latticeengines.apps.cdl.mds.ExternalSystemMetadataStore;
import com.latticeengines.apps.cdl.mds.RatingDisplayMetadataStore;
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
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("systemMetadataStore")
public class SystemMetadataStoreImpl extends
        DecoratedMetadataStore<//
                Namespace2<BusinessEntity, DataCollection.Version>, //
                Namespace2<BusinessEntity, DataCollection.Version>, //
                Namespace2<BusinessEntity, DataCollection.Version>>
        implements SystemMetadataStore {

    @Inject
    public SystemMetadataStoreImpl( //
            RawSystemMetadataStoreImpl rawSystemMetadataStore, //
            AccountAttrsDecoratorFacImpl accountAttrsDecorator, //
            ContactAttrsDecoratorFacImpl contactAttrsDecorator, //
            ActivityMetricsDecoratorFac activityMetricsDecorator, //
            RatingDisplayMetadataStore ratingDisplayMetadataStore, //
            ExternalSystemMetadataStore externalSystemMetadataStore, //
            CuratedAttrsMetadataStore derivedAttrsMetadataStore) {
        super(rawSystemMetadataStore, //
                getDecoratorChain(//
                        accountAttrsDecorator, //
                        contactAttrsDecorator, //
                        activityMetricsDecorator, //
                        ratingDisplayMetadataStore, //
                        externalSystemMetadataStore, //
                        derivedAttrsMetadataStore //
                ));
    }

    private static ChainedDecoratorFactory<Namespace2<BusinessEntity, DataCollection.Version>> getDecoratorChain(
            AccountAttrsDecoratorFacImpl accountAttrsDecorator, //
            ContactAttrsDecoratorFacImpl contactAttrsDecorator, //
            ActivityMetricsDecoratorFac activityMetricsDecorator, //
            RatingDisplayMetadataStore ratingDisplayMetadataStore, //
            ExternalSystemMetadataStore externalSystemMetadataStore, //
            CuratedAttrsMetadataStore curatedAttrsMetadataStore) {
        DecoratorFactory<Namespace1<String>> ratingDisplayDecorator = //
                MdsDecoratorFactory.fromMds("RatingDisplay", ratingDisplayMetadataStore);
        DecoratorFactory<Namespace2<String, BusinessEntity>> lookupIdDecorator = //
                MdsDecoratorFactory.fromMds("LookupId", externalSystemMetadataStore);
        DecoratorFactory<Namespace2<String, DataCollection.Version>> curatedAttrsDecorator = //
                MdsDecoratorFactory.fromMds("CuratedAttrs", curatedAttrsMetadataStore);
        Decorator apsAttrDecorator = new APSAttrsDecorator();
        Decorator postRenderDecorator = new SystemPostRenderDecorator();
        // order in sync with ChainedDecoratorFactory.project() below
        List<DecoratorFactory<? extends Namespace>> factories = Arrays.asList(//
                accountAttrsDecorator, //
                lookupIdDecorator, //
                contactAttrsDecorator, //
                activityMetricsDecorator, //
                apsAttrDecorator, //
                ratingDisplayDecorator, //
                curatedAttrsDecorator, //
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
                Namespace activityMetricsNs = Namespace
                        .as(BusinessEntity.PurchaseHistory.equals(entity) ? tenantId : "");
                Namespace ratingNs = Namespace.as(BusinessEntity.Rating.equals(entity) ? tenantId : "");
                Namespace curatedNs = Namespace.as(//
                        BusinessEntity.CuratedAccount.equals(entity) ? tenantId : "", //
                        namespace.getCoord2());
                Namespace lookupIdNs = Namespace.as(tenantId, entity);
                // order in sync with getDecoratorChain()
                return Arrays.asList( //
                        accountNs, //
                        lookupIdNs, //
                        contactNs, //
                        activityMetricsNs, //
                        Namespace0.NS, //
                        ratingNs, //
                        curatedNs, //
                        Namespace0.NS //
                );
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
