package com.latticeengines.apps.cdl.mds.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.RatingDisplayDecorator;
import com.latticeengines.apps.cdl.mds.RatingServingStoreTemplate;
import com.latticeengines.apps.cdl.mds.SystemServingStoreTemplate;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datatemplate.DecoratedDataTemplate;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace0;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component
public class RatingServingStoreTemplateImpl extends
        DecoratedDataTemplate<Namespace1<DataCollection.Version>, Namespace2<BusinessEntity, DataCollection.Version>, Namespace0>
        implements RatingServingStoreTemplate {

    @Inject
    public RatingServingStoreTemplateImpl(SystemServingStoreTemplate systemServingStoreTemplate, RatingDisplayDecorator ratingDisplayDecorator) {
        super(systemServingStoreTemplate, ratingDisplayDecorator);
    }

    @Override
    protected Namespace2<BusinessEntity, DataCollection.Version> projectBaseNamespace(
            Namespace1<DataCollection.Version> namespace) {
        return Namespace.as(BusinessEntity.Rating, namespace.getCoord1());
    }

    @Override
    protected Namespace0 projectDecoratorNamespace(
            Namespace1<DataCollection.Version> namespace) {
        return new Namespace0();
    }

}
