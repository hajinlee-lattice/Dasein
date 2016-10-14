package com.latticeengines.modelquality.controller;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
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
        return getAll();
    }

    @Override
    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create PropData configuration")
    public String createPropDataConfig(@RequestBody PropData propDataConfig) {
        return create(propDataConfig);
    }

    @Override
    @RequestMapping(value = "/latest", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create PropData configuration from production")
    public PropData createPropDataConfigFromProduction() {
        return createForProduction();
    }

    @Override
    @RequestMapping(value = "/{propDataConfigName:.*}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get PropData configuration by name")
    public PropData getPropDataConfigByName(@PathVariable String propDataConfigName) {
        return getByName(propDataConfigName);
    }

    public PropData createForProduction() {
        return propDataService.createLatestProductionPropData();
    }

    @Override
    public PropData getByName(String name) {
        return propDataEntityMgr.findByName(name);
    }

    @Override
    public List<PropData> getAll() {
        return propDataEntityMgr.findAll();
    }

    @Override
    public String create(PropData config, Object... params) {
        propDataEntityMgr.create(config);
        return config.getName();
    }

}
