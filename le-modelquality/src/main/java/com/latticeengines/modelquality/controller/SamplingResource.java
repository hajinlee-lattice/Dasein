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
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.modelquality.entitymgr.SamplingEntityMgr;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource to get samplings parameters")
@RestController
public class SamplingResource {

    @Autowired
    private SamplingEntityMgr samplingEntityMgr;

    private static final Log log = LogFactory.getLog(SamplingResource.class);

    @RequestMapping(value = "/samplingconfigs", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Sampling Configs")
    public ResponseDocument<List<Sampling>> getSamplings() {
        try {
            List<Sampling> samplings = samplingEntityMgr.findAll();
            return ResponseDocument.successResponse(samplings);
        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/samplingconfigs", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upsert Sampling Configs")
    public ResponseDocument<String> upsertSamplings(@RequestBody List<Sampling> samplings) {
        try {
            samplingEntityMgr.deleteAll();
            samplingEntityMgr.createSamplings(samplings);
            return ResponseDocument.successResponse("OK");
        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }

}
