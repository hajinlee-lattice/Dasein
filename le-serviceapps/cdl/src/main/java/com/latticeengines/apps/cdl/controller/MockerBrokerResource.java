package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.MockBrokerInstanceService;
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "MockBroker", description = "REST resource for mock broker.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/mockbroker")
public class MockerBrokerResource {

    @Inject
    private MockBrokerInstanceService mockBrokerInstanceService;

    @PostMapping("")
    @ApiOperation(value = "create or update mock broker")
    public MockBrokerInstance createOrUpdate(@PathVariable String customerSpace, @RequestBody MockBrokerInstance mockBrokerInstance) {
        return mockBrokerInstanceService.createOrUpdate(mockBrokerInstance);
    }

}
