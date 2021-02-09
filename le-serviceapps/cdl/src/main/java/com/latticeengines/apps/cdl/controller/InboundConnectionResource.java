package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.InboundConnectionService;
import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "inboundconnection", description = "REST resource for inbound connection")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/inboundconnection")
public class InboundConnectionResource {

    private static final Logger log = LoggerFactory.getLogger(InboundConnectionResource.class);

    @Inject
    private InboundConnectionService inboundConnectionService;

    @PostMapping("/listdocumenttypes")
    @ApiOperation(value = "list document type for a broker")
    public List<String> listDocumentTypes(@PathVariable String customerSpace, @RequestBody BrokerReference brokerReference) {
        return inboundConnectionService.listDocumentTypes(brokerReference);
    }

    @PostMapping("/desdocumenttype")
    @ApiOperation(value = "get schema for a broker with certain entity")
    public List<ColumnMetadata> describeDocumentType(@PathVariable String customerSpace, @RequestParam(value = "documentType") String documentType,
                                                     @RequestBody BrokerReference brokerReference) {
        return inboundConnectionService.describeDocumentType(brokerReference, documentType);
    }

    @PostMapping("/setupbroker")
    @ApiOperation(value = "set up broker")
    public BrokerReference setUpBroker(@PathVariable String customerSpace, @RequestBody BrokerReference brokerReference) {
        return inboundConnectionService.setUpBroker(brokerReference);
    }

    @PostMapping("/brokerreference")
    @ApiOperation(value = "get broker reference by broker reference")
    public BrokerReference getBrokerReference(@PathVariable String customerSpace, @RequestBody BrokerReference brokerReference) {
        return inboundConnectionService.getBrokerReference(brokerReference);
    }

    @PostMapping("")
    @ApiOperation(value = "update broker by broker reference")
    public BrokerReference updateBroker(@PathVariable String customerSpace, @RequestBody BrokerReference brokerReference) {
        return inboundConnectionService.updateBroker(brokerReference);
    }

    @PostMapping("/schedule")
    @ApiOperation(value = "get broker reference by broker reference")
    public void schedule(@PathVariable String customerSpace, @RequestBody BrokerReference brokerReference) {
        inboundConnectionService.schedule(brokerReference);
    }

}
