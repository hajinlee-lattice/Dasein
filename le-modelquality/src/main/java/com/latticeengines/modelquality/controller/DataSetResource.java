package com.latticeengines.modelquality.controller;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
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
    private static final Log log = LogFactory.getLog(DataSetResource.class);

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
    @RequestMapping(value = "/createFromTenant", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert new DataSet for given tenant")
    public String createDataSetFromTenant(@RequestParam("tenantId") String tenantName,
            @RequestParam("tenantType") DataSetTenantType tenantType,
            @RequestParam(value = "modelID", required = false) String modelID,
            @RequestParam(value = "playExternalID", required = false) String playExternalID) {
        switch(tenantType){
        case LP2:
            if(modelID == null || modelID.isEmpty()){
                throw new LedpException(LedpCode.LEDP_35004, new String[]{ "Model ID", "LP2"});
            }
            dataSetService.createDataSetFromLP2Tenant(tenantName, modelID);
            break;
        case LPI:
            if(modelID == null || modelID.isEmpty()){
                throw new LedpException(LedpCode.LEDP_35004, new String[]{ "Model ID", "LPI"});
            }
            dataSetService.createDataSetFromLPITenant(tenantName, modelID);
            break;
        case PLAYMAKER:
            if(playExternalID == null || playExternalID.isEmpty()){
                throw new LedpException(LedpCode.LEDP_35004, new String[]{ "Play ExternalID", "PLAYMAKER"});
            }
            dataSetService.createDataSetFromPlaymakerTenant(tenantName, playExternalID);
        }
        return null;
        
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
