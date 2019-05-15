package com.latticeengines.pls.controller.datacollection;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "accounts", description = "REST resource for serving data about accounts")
@RestController
@RequestMapping("/accounts")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class AccountResource extends BaseFrontEndEntityResource {

    private static final Logger log = LoggerFactory.getLogger(AccountResource.class);

    @Inject
    private BatonService batonService;

    @Inject
    public AccountResource(EntityProxy entityProxy, SegmentProxy segmentProxy, DataCollectionProxy dataCollectionProxy,
                           GraphDependencyToUIActionUtil graphDependencyToUIActionUtil) {
        super(entityProxy, segmentProxy, dataCollectionProxy, graphDependencyToUIActionUtil);
    }

    @Deprecated
    @Override
    @RequestMapping(value = "/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public Long getCount(@RequestBody(required = false) FrontEndQuery frontEndQuery) {
        try {
            return super.getCount(frontEndQuery);
        } catch (Exception e) {
            log.error("Failed to get account data", e);
            throw new LedpException(LedpCode.LEDP_36002);
        }
    }

    @Override
    @RequestMapping(value = "/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@RequestBody(required = false) FrontEndQuery frontEndQuery) {
        try {
            return super.getData(frontEndQuery);
        } catch (Exception e) {
            log.error("Failed to get account data", e);
            throw new LedpException(LedpCode.LEDP_36002);
        }
    }

    @Override
    BusinessEntity getMainEntity() {
        return BusinessEntity.Account;
    }

    @Override
    List<Lookup> getDataLookups() {
        if (isEntityMatchEnabled()) {
            return Arrays.asList( //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.CompanyName.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.Website.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.City.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.State.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.Country.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.CustomerAccountId.name()));
        } else {
            return Arrays.asList( //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.CompanyName.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.Website.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.City.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.State.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.Country.name()), //
                    new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name()));
        }
    }

    @Override
    protected void postProcessRecord(Map<String, Object> result) {
        overwriteCompanyName(result);
        if (isEntityMatchEnabled() && result.containsKey(InterfaceName.CustomerAccountId.name())) {
            result.put(InterfaceName.AccountId.name(), result.get(InterfaceName.CustomerAccountId.name()));
            result.remove(InterfaceName.CustomerAccountId.name());
        }
    }

    private boolean isEntityMatchEnabled() {
        return batonService.isEnabled(MultiTenantContext.getCustomerSpace(), LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
    }

}
