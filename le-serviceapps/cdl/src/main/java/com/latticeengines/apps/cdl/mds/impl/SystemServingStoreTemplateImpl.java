package com.latticeengines.apps.cdl.mds.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.RatingDisplayMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemServingStoreTemplate;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.apps.core.mds.AMMetadataStore;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datatemplate.DataTemplate;
import com.latticeengines.domain.exposed.metadata.datatemplate.DataTemplateName;
import com.latticeengines.domain.exposed.metadata.datatemplate.DecoratedDataTemplate;
import com.latticeengines.domain.exposed.metadata.mds.ChainedDecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.DecoratorFactory1;
import com.latticeengines.domain.exposed.metadata.mds.MdsDecoratorFactory;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.metadata.DataTemplateProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@Component
public class SystemServingStoreTemplateImpl extends
        DecoratedDataTemplate<Namespace2<BusinessEntity, DataCollection.Version>, Namespace2<String, String>, Namespace1<BusinessEntity>>
        implements SystemServingStoreTemplate {

    private final CDLNamespaceService cdlNamespaceService;

    @Inject
    public SystemServingStoreTemplateImpl(DataTemplateProxy dataTemplateProxy, AMMetadataStore amMetadataStore,
            CDLNamespaceService cdlNamespaceService, RatingDisplayMetadataStore ratingDisplayMetadataStore) {
        super(getBaseTemplate(dataTemplateProxy),
                getDecoratorChain(cdlNamespaceService, amMetadataStore, ratingDisplayMetadataStore));
        this.cdlNamespaceService = cdlNamespaceService;
    }

    private static DataTemplate<Namespace2<String, String>> getBaseTemplate(DataTemplateProxy proxy) {
        return proxy.toDataTemplate(DataTemplateName.Table, String.class, String.class);
    }

    private static ChainedDecoratorFactory<Namespace1<BusinessEntity>> getDecoratorChain(
            CDLNamespaceService cdlNamespaceService, AMMetadataStore amMetadataStore,
            RatingDisplayMetadataStore ratingDisplayMetadataStore) {
        DecoratorFactory<Namespace1<BusinessEntity>> staticDecorator = getStaticDecorator();
        DecoratorFactory<Namespace1<String>> amDecorator = getAMDecorator(amMetadataStore);
        DecoratorFactory<Namespace1<String>> ratingDisplayDecorator = getRatingDecorator(ratingDisplayMetadataStore);
        List<DecoratorFactory<? extends Namespace>> factories = Arrays.asList( //
                staticDecorator, //
                amDecorator, //
                ratingDisplayDecorator //
        );

        return new ChainedDecoratorFactory<Namespace1<BusinessEntity>>("SystemServingStoreChain", factories) {
            @Override
            protected List<Namespace> project(Namespace1<BusinessEntity> namespace) {
                BusinessEntity entity = namespace.getCoord1();
                String tenantId = MultiTenantContext.getTenantId();
                Namespace staticNs = Namespace.as(entity);
                Namespace amNs = cdlNamespaceService.resolveDataCloudVersion();
                Namespace ratingNs = Namespace.as(BusinessEntity.Rating.equals(entity) ? tenantId : "");
                return Arrays.asList(staticNs, amNs, ratingNs);
            }
        };
    }

    private static DecoratorFactory<Namespace1<String>> getAMDecorator(AMMetadataStore amMetadataStore) {
        return MdsDecoratorFactory.fromMds("AMDecorator", amMetadataStore);
    }

    @SuppressWarnings("unchecked")
    private static DecoratorFactory<Namespace1<String>> getRatingDecorator(
            RatingDisplayMetadataStore ratingDisplayMetadataStore) {
        return MdsDecoratorFactory.fromMds("RatingDisplay", ratingDisplayMetadataStore);
    }

    private static DecoratorFactory<Namespace1<BusinessEntity>> getStaticDecorator() {
        return (DecoratorFactory1<BusinessEntity>) namespace -> new Decorator() {
            @Override
            public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
                return metadata.map(cm -> {
                    cm.setEntity(namespace.getCoord1());
                    cm.enableGroup(ColumnSelection.Predefined.Segment);
                    return cm;
                });
            }

            @Override
            public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
                return metadata.map(cm -> {
                    cm.setEntity(namespace.getCoord1());
                    cm.enableGroup(ColumnSelection.Predefined.Segment);
                    return cm;
                });
            }

            @Override
            public String getName() {
                return "ServingStoreStatic";
            }
        };
    }

    @Override
    protected Namespace2<String, String> projectBaseNamespace(
            Namespace2<BusinessEntity, DataCollection.Version> namespace) {
        return cdlNamespaceService.resolveServingStore(namespace);
    }

    @Override
    protected Namespace1<BusinessEntity> projectDecoratorNamespace(
            Namespace2<BusinessEntity, DataCollection.Version> namespace) {
        return Namespace.as(namespace.getCoord1());
    }

}
