package com.latticeengines.modelquality.controller;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.entitymgr.PropDataEntityMgr;
import com.latticeengines.modelquality.service.PropDataService;
import com.latticeengines.network.exposed.modelquality.ModelQualityPropDataInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource to get PropData parameters")
@RestController
@RequestMapping("/propdataconfigs")
public class PropDataResource implements ModelQualityPropDataInterface, CrudInterface<PropData> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(PropDataResource.class);

    @Autowired
    private PropDataEntityMgr propDataEntityMgr;
    
    @Autowired
    private PropDataService propDataService;

    @Override
    @RequestMapping(value = "/", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get PropData configurations")
    public List<PropData> getPropDataConfigs() {
        return propDataEntityMgr.findAll();
    }

    @Override
    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create PropData configuration")
    public String createPropDataConfig(@RequestBody PropData propDataConfig) {
        propDataEntityMgr.create(propDataConfig);
        return propDataConfig.getName();
    }

    @Override
    @RequestMapping(value = "/latest", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create PropData configuration from production")
    public PropData createPropDataConfigFromProduction() {
        return propDataService.createLatestProductionPropData();
    }
    
    @Override
    @RequestMapping(value = "/{propDataConfigName}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get PropData configuration by name")
    public PropData getPropDataConfigByName(String propDataConfigName) {
        return propDataEntityMgr.findByName(propDataConfigName);
    }

    @Override
    public PropData createForProduction() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PropData getByName(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<PropData> getAll() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String create(PropData config, Object... params) {
        // TODO Auto-generated method stub
        return null;
    }
    
}
