package com.latticeengines.apps.cdl.mds.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.AccountAttrDecoratorFac;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DummyDecorator;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;


@Component("AccountAttrDecorator")
public class AccountAttrsDecoratorFac implements AccountAttrDecoratorFac {

    @Inject
    private ZKConfigService zkConfigService;

    @Override
    public Decorator getDecorator(Namespace1<String> namespace) {
        String tenantId = namespace.getCoord1();
        if (StringUtils.isNotBlank(tenantId)) {
            boolean internalEnrichEnabled = zkConfigService.isInternalEnrichmentEnabled(CustomerSpace.parse(tenantId));
            return new AccountAttrsDecorator(internalEnrichEnabled);
        } else {
            return new DummyDecorator();
        }
    }
}
