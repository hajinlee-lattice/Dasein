package com.latticeengines.app.exposed.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.DataCollectionPrecheckService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.DataCollectionPrechecks;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datacollection", description = "REST resource for default metadata data collection")
@RestController
@RequestMapping("/datacollection")
public class DataCollectionResource {
    public static final String ATTR_CONFIG_PATH = "";

    private final DataCollectionProxy dataCollectionProxy;

    private final DataCollectionPrecheckService dataCollectionPrecheckService;

    @Inject
    public DataCollectionResource(DataCollectionProxy dataCollectionProxy,
            DataCollectionPrecheckService dataCollectionPrecheckService) {
        this.dataCollectionProxy = dataCollectionProxy;
        this.dataCollectionPrecheckService = dataCollectionPrecheckService;
    }

    @GetMapping("/status")
    @ResponseBody
    @ApiOperation(value = "Get attr data collection status")
    public DataCollectionStatus getCollectionStatus(
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return dataCollectionProxy.getOrCreateDataCollectionStatus(MultiTenantContext.getCustomerSpace().toString(),
                version);
    }

    @GetMapping("/precheck")
    @ApiOperation(value = "Check whether Account, Product, Transaction and their attributes exist in serving store.")
    public DataCollectionPrechecks precheck() {
        return dataCollectionPrecheckService
                .validateDataCollectionPrechecks(MultiTenantContext.getCustomerSpace().toString());
    }
}
