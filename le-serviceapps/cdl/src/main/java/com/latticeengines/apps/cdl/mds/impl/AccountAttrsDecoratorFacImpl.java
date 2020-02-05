package com.latticeengines.apps.cdl.mds.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.AccountAttrsDecoratorFac;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DummyDecorator;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;


@Component("AccountAttrDecorator")
public class AccountAttrsDecoratorFacImpl implements AccountAttrsDecoratorFac {

    private static final Logger log = LoggerFactory.getLogger(AccountAttrsDecoratorFacImpl.class);
    @Inject
    private ZKConfigService zkConfigService;

    @Inject
    private BatonService batonService;

    @Inject
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @Inject
    private CDLProxy cdlProxy;

    @Override
    public Decorator getDecorator(Namespace1<String> namespace) {
        String tenantId = namespace.getCoord1();
        if (StringUtils.isNotBlank(tenantId)) {
            boolean internalEnrichEnabled = zkConfigService.isInternalEnrichmentEnabled(CustomerSpace.parse(tenantId));
            boolean entityMatchEnabled = batonService.isEntityMatchEnabled(CustomerSpace.parse(tenantId));
            boolean onlyEntityMatchGAEnabled = batonService.onlyEntityMatchGAEnabled(CustomerSpace.parse(tenantId));
            List<S3ImportSystem> importSystems = cdlProxy.getS3ImportSystemList(tenantId);
            CDLExternalSystem externalSystem = cdlExternalSystemProxy.getCDLExternalSystem(tenantId,
                    BusinessEntity.Account.name());
            Set<String> attrNameInOtherIDAndMatchID = new HashSet<>();
            if (CollectionUtils.isNotEmpty(importSystems)) {
                importSystems.forEach(e -> {
                    if (StringUtils.isNotEmpty(e.getAccountSystemId())) {
                        attrNameInOtherIDAndMatchID.add(e.getAccountSystemId());
                    }});
            }
            if (externalSystem != null && StringUtils.isNotBlank(externalSystem.getIdMapping())) {
                List<Pair<String, String>> idList = externalSystem.getIdMappingList();
                idList.forEach(e -> attrNameInOtherIDAndMatchID.add(e.getLeft()));
            }
            log.info("special attribute name in other Id and match Id " + attrNameInOtherIDAndMatchID);
            return new AccountAttrsDecorator(internalEnrichEnabled, entityMatchEnabled, onlyEntityMatchGAEnabled,
                    attrNameInOtherIDAndMatchID);
        } else {
            return new DummyDecorator();
        }
    }
}
