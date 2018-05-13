package com.latticeengines.pls.controller.datacollection;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusDetail;
import com.latticeengines.proxy.exposed.cdl.DataCollectionStatusProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datacollection", description = "REST resource for default metadata data collection")
@RestController
@RequestMapping("/datacollection/status")
public class DataCollectionStatusResource {

    @Inject
    private DataCollectionStatusProxy dataCollectionStatusProxy;
    @RequestMapping(value = "", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get attr data collection status")
    public DataCollectionStatusDetail getCollectionStatus(HttpServletRequest request) {
        DataCollectionStatusDetail detail = dataCollectionStatusProxy
                .getOrCreateDataCollectionStatus(MultiTenantContext.getCustomerSpace().toString());
        return detail;
    }
}
