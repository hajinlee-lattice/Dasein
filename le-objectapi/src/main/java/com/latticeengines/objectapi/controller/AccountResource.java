package com.latticeengines.objectapi.controller;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.network.exposed.objectapi.AccountInterface;
import com.latticeengines.objectapi.service.AccountQueryService;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "accounts", description = "REST resource for accounts")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/accounts")
public class AccountResource implements AccountInterface {

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private QueryEvaluator queryEvaluator;

    @Autowired
    private AccountQueryService accountQueryService;

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(AccountResource.class);

    /*
     * Based on
     * https://confluence.lattice-engines.com/display/ENG/PlayMakerAPI+-+Datastore+proposal+for+Recommendation%
     * 2C+Account+Extension+and+Plays
     * 
     */
    @RequestMapping(value = "/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve Account data for the specified parameters; by default returns AccountId, LatticeAccountId, SalesforceAccountID, LastModified ")
    @Override
    public DataPage getAccounts(@PathVariable String customerSpace,
            @ApiParam(value = "The UTC timestamp of last modification in ISO8601 format", required = false) @RequestParam(value = "start", required = false) String start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) Integer offset,
            @ApiParam(value = "Number of records returned above offset (max is 250 records per request)", required = true) @RequestParam(value = "pageSize", required = true) Integer pageSize,
            @RequestBody DataRequest dataRequest) {
        Query query = accountQueryService.generateAccountQuery(start, offset, pageSize, dataRequest);

        DataPage dataPage = null;
        try (PerformanceTimer timer = new PerformanceTimer("fetch data")) {
            List<Map<String, Object>> results = queryEvaluator
                    .run(dataCollectionProxy.getDefaultAttributeRepository(customerSpace), query).getData();
            dataPage = new DataPage(results);
        }

        return dataPage;
    }

}
