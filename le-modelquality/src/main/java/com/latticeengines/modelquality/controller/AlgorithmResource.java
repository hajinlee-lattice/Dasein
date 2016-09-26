package com.latticeengines.modelquality.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.modelquality.entitymgr.AlgorithmEntityMgr;
import com.latticeengines.modelquality.service.AlgorithmService;
import com.latticeengines.network.exposed.modelquality.ModelQualityAlgorithmInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource to get algorithms parameters")
@RestController
@RequestMapping("/algorithms")
public class AlgorithmResource implements ModelQualityAlgorithmInterface, CrudInterface<Algorithm> {

    @Autowired
    private AlgorithmService algorithmService;

    @Autowired
    private AlgorithmEntityMgr algorithmEntityMgr;

    @Override
    @RequestMapping(value = "/", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Algorithms")
    public List<Algorithm> getAlgorithms() {
        return getAll();
    }

    @Override
    @RequestMapping(value = "/latest", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upsert Algorithms")
    public Algorithm createAlgorithmFromProduction() {
        return createForProduction();
    }

    @Override
    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create Algorithm")
    public String createAlgorithm(@RequestBody Algorithm algorithm) {
        return create(algorithm);
    }

    @Override
    @RequestMapping(value = "/{algorithmName}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Algorithm by name")
    public Algorithm getAlgorithmByName(@PathVariable String algorithmName) {
        return getByName(algorithmName);
    }

    public Algorithm createForProduction() {
        return algorithmService.createLatestProductionAlgorithm();
    }

    @Override
    public Algorithm getByName(String name) {
        return algorithmEntityMgr.findByName(name);
    }

    @Override
    public List<Algorithm> getAll() {
        return algorithmEntityMgr.findAll();
    }

    @Override
    public String create(Algorithm config, Object... params) {
        algorithmEntityMgr.create(config);
        return config.getName();
    }

}
