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

import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.modelquality.entitymgr.SamplingEntityMgr;
import com.latticeengines.modelquality.service.SamplingService;
import com.latticeengines.network.exposed.modelquality.ModelQualitySamplingInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource to get samplings parameters")
@RestController
public class SamplingResource implements ModelQualitySamplingInterface, CrudInterface<Sampling> {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(SamplingResource.class);

    @Autowired
    private SamplingEntityMgr samplingEntityMgr;

    @Autowired
    private SamplingService samplingService;

    @Override
    @RequestMapping(value = "/samplingconfigs", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get list of sampling configurations")
    public List<Sampling> getSamplingConfigs() {
        return getAll();
    }

    @Override
    @RequestMapping(value = "/samplingconfigs/{samplingConfigName:.*}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get list of sampling configurations")
    public Sampling getSamplingConfigByName(@PathVariable String samplingConfigName) {
        return getByName(samplingConfigName);
    }

    @Override
    @RequestMapping(value = "/samplingconfigs", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create sampling configuration")
    public String createSamplingConfig(@RequestBody Sampling samplingConfig) {
        return create(samplingConfig);
    }

    @Override
    @RequestMapping(value = "/samplingconfigs/latest", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create production sampling config")
    public Sampling createSamplingFromProduction() {
        return createForProduction();
    }

    public Sampling createForProduction() {
        return samplingService.createLatestProductionSamplingConfig();
    }

    @Override
    public Sampling getByName(String name) {
        return samplingEntityMgr.findByName(name);
    }

    @Override
    public List<Sampling> getAll() {
        return samplingEntityMgr.findAll();
    }

    @Override
    public String create(Sampling config, Object... params) {
        samplingEntityMgr.create(config);
        return config.getName();
    }

}
