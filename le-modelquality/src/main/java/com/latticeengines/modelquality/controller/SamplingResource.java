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

import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.modelquality.entitymgr.SamplingEntityMgr;
import com.latticeengines.modelquality.service.SamplingService;
import com.latticeengines.network.exposed.modelquality.ModelQualitySamplingInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource to get samplings parameters")
@RestController
public class SamplingResource implements ModelQualitySamplingInterface {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(SamplingResource.class);

    @Autowired
    private SamplingEntityMgr samplingEntityMgr;
    
    @Autowired
    private SamplingService samplingService;

    @Override
    @RequestMapping(value = "/samplingconfigs", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get list of sampling configurations")
    public List<Sampling> getSamplingConfigs() {
        return samplingEntityMgr.findAll();
    }

    @Override
    @RequestMapping(value = "/samplingconfigs/{samplingConfigName}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get list of sampling configurations")
    public Sampling getSamplingConfigByName(@PathVariable String samplingConfigName) {
        return samplingEntityMgr.findByName(samplingConfigName);
    }

    @Override
    @RequestMapping(value = "/samplingconfigs", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create sampling configuration")
    public String createSamplingConfig(@RequestBody Sampling samplingConfig) {
        samplingEntityMgr.create(samplingConfig);
        return samplingConfig.getName();
    }

    @Override
    @RequestMapping(value = "/samplingconfigs/latest", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create production sampling config")
    public Sampling createSamplingFromProduction() {
        return samplingService.createLatestProductionSamplingConfig();
    }

}
