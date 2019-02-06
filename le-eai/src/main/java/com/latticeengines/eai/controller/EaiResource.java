package com.latticeengines.eai.controller;

import java.util.Collections;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.network.exposed.eai.EaiInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "eaijobs", description = "REST resource for importing/exporting data into/from Lattice")
@RestController
@RequestMapping("")
public class EaiResource implements EaiInterface {

    @Inject
    private EaiService eaiService;

    @RequestMapping(value = "/jobs", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Submit an eai job")
    public AppSubmission submitEaiJob(@RequestBody EaiJobConfiguration eaiJobConfig) {
        return new AppSubmission(Collections.singletonList(eaiService.submitEaiJob(eaiJobConfig)));
    }
}
