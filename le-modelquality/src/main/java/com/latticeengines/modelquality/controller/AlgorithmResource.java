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

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.modelquality.entitymgr.AlgorithmEntityMgr;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource to get algorithms parameters")
@RestController
public class AlgorithmResource {

    private static final Log log = LogFactory.getLog(AlgorithmResource.class);

    @Autowired
    private AlgorithmEntityMgr algorithmEntityMgr;

    @RequestMapping(value = "/algorithms", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Algorithms")
    public ResponseDocument<List<Algorithm>> getAlgorithms() {
        try {

            List<Algorithm> algorithms = algorithmEntityMgr.findAll();
            return ResponseDocument.successResponse(algorithms);
        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/algorithms", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upsert Algorithms")
    public ResponseDocument<String> upsertAlgorithms(@RequestBody List<Algorithm> algorithms) {
        try {
            algorithmEntityMgr.deleteAll();
            algorithmEntityMgr.createAlgorithms(algorithms);
            return ResponseDocument.successResponse("OK");
        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }

}
