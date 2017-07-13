package com.latticeengines.modelquality.controller;

import java.util.List;

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

import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.DataSetTenantType;
import com.latticeengines.modelquality.entitymgr.DataSetEntityMgr;
import com.latticeengines.modelquality.service.DataSetService;
import com.latticeengines.network.exposed.modelquality.ModelQualityDataSetInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource to get DataSet parameters")
@RestController
@RequestMapping("/datasets")
public class DataSetResource implements ModelQualityDataSetInterface, CrudInterface<DataSet> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DataSetResource.class);

    @Autowired
    private DataSetEntityMgr dataSetEntityMgr;

    @Autowired
    private DataSetService dataSetService;

    @Override
    @RequestMapping(value = "/", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get DataSets")
    public List<DataSet> getDataSets() {
        return getAll();
    }

    @Override
    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert new DataSet")
    public String createDataSet(@RequestBody DataSet dataSet) {
        return create(dataSet);
    }

    @Override
    @RequestMapping(value = "/create", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert new DataSet for given tenant")
    public String createDataSetFromTenant(@RequestParam("tenantType") DataSetTenantType tenantType,
            @RequestParam("tenantId") String tenantId, @RequestParam("sourceId") String sourceId) {
        String toReturn = null;
        switch (tenantType) {
        case LP2:
            toReturn = dataSetService.createDataSetFromLP2Tenant(tenantId, sourceId);
            break;
        case LPI:
            toReturn = dataSetService.createDataSetFromLPITenant(tenantId, sourceId);
            break;
        case PLAYMAKER:
            toReturn = dataSetService.createDataSetFromPlaymakerTenant(tenantId, sourceId);
        }
        return toReturn;
    }

    @Override
    @RequestMapping(value = "/{dataSetName:.*}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get DataSet by name")
    public DataSet getDataSetByName(@PathVariable String dataSetName) {
        return getByName(dataSetName);
    }

    @Override
    public DataSet getByName(String name) {
        return dataSetEntityMgr.findByName(name);
    }

    @Override
    public List<DataSet> getAll() {
        return dataSetEntityMgr.findAll();
    }

    @Override
    public String create(DataSet config, Object... params) {
        dataSetEntityMgr.create(config);
        return config.getName();
    }
}
