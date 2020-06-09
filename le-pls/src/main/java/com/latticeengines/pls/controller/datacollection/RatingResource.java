package com.latticeengines.pls.controller.datacollection;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ratings", description = "REST resource for serving data about ratings")
@RestController
@RequestMapping("/ratings")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class RatingResource {

    private static final Logger log = LoggerFactory.getLogger(RatingResource.class);

    @Inject
    private RatingProxy ratingProxy;

    @Deprecated
    @PostMapping("/count")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public Long getCount(@RequestBody FrontEndQuery frontEndQuery) {
        try {
            String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
            frontEndQuery.setMainEntity(BusinessEntity.Account);
            return ratingProxy.getCountFromObjectApi(tenantId, frontEndQuery, null);
        } catch (Exception e) {
            log.error("Failed to get rating data", e);
            throw new LedpException(LedpCode.LEDP_36002);
        }
    }

    @PostMapping("/data")
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@RequestBody FrontEndQuery frontEndQuery) {
        try {
            String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
            frontEndQuery.setMainEntity(BusinessEntity.Account);
            return ratingProxy.getData(tenantId, frontEndQuery);
        } catch (Exception e) {
            log.error("Failed to get rating data", e);
            throw new LedpException(LedpCode.LEDP_36002);
        }
    }
}
