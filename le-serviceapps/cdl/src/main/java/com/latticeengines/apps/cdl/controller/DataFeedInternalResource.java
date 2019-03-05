package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.security.TenantStatus;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafeed_internal", description = "REST resource for metadata data feed internal operations")
@RestController
@RequestMapping("/datafeed/internal")
public class DataFeedInternalResource {

    @Inject
    private DataFeedService dataFeedService;

    @RequestMapping(value = "/list", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @NoCustomerSpace
    @ApiOperation(value = "get all data feeds.")
    public List<DataFeed> getAllDataFeeds() {
        return dataFeedService.getAllDataFeeds();
    }

    @RequestMapping(value = "/simpledatafeedlist", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @NoCustomerSpace
    @ApiOperation(value = "get all simple data feeds.")
    public List<SimpleDataFeed> getAllSimpleDataFeeds(
            @RequestParam(value = "status", required = false, defaultValue = "")String tenantStatus) {
        if (StringUtils.isEmpty((tenantStatus))) {
            return dataFeedService.getAllSimpleDataFeeds();
        } else {
            return dataFeedService.getSimpleDataFeedsByTenantStatus(TenantStatus.valueOf(tenantStatus));
        }

    }
}
