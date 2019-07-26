package com.latticeengines.apps.cdl.mds.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.ContactAttrsDecoratorFac;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DummyDecorator;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;

@Component("ContactAttrDecorator")
public class ContactAttrsDecoratorFacImpl implements ContactAttrsDecoratorFac {

    @Inject
    private BatonService batonService;

    @Override
    public Decorator getDecorator(Namespace1<String> namespace) {
        String tenantId = namespace.getCoord1();
        if (StringUtils.isNotBlank(tenantId)) {
            boolean entityMatchEnabled = batonService.isEntityMatchEnabled(CustomerSpace.parse(tenantId));
            return new ContactAttrsDecorator(entityMatchEnabled);
        } else {
            return new DummyDecorator();
        }
    }

}
