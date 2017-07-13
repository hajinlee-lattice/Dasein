package com.latticeengines.modelquality.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.modelquality.entitymgr.DataFlowEntityMgr;
import com.latticeengines.modelquality.service.DataFlowService;
import com.latticeengines.network.exposed.modelquality.ModelQualityDataFlowInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource to get DataFlow parameters")
@RestController
@RequestMapping("/dataflows")
public class DataFlowResource implements ModelQualityDataFlowInterface, CrudInterface<DataFlow> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DataFlowResource.class);

    @Autowired
    private DataFlowEntityMgr dataFlowEntityMgr;

    @Autowired
    private DataFlowService modelQualityDataFlowService;

    @Override
    @RequestMapping(value = "/", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get DataFlows")
    public List<DataFlow> getDataFlows() {
        return getAll();
    }

    @Override
    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create DataFlow")
    public String createDataFlow(@RequestBody DataFlow dataFlow) {
        return create(dataFlow);
    }

    @Override
    @RequestMapping(value = "/{dataFlowName:.*}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get DataFlows")
    public DataFlow getDataFlowByName(@PathVariable String dataFlowName) {
        return getByName(dataFlowName);
    }

    @Override
    @RequestMapping(value = "/latest", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create production data flow config")
    public DataFlow createDataFlowFromProduction() {
        return createForProduction();
    }

    public DataFlow createForProduction() {
        return modelQualityDataFlowService.createLatestProductionDataFlow();
    }

    @Override
    public DataFlow getByName(String name) {
        return dataFlowEntityMgr.findByName(name);
    }

    @Override
    public List<DataFlow> getAll() {
        return dataFlowEntityMgr.findAll();
    }

    @Override
    public String create(DataFlow config, Object... params) {
        dataFlowEntityMgr.create(config);
        return config.getName();
    }

}
