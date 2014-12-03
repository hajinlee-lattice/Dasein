package com.latticeengines.api.controller;

import java.util.Arrays;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dataplatform.exposed.service.EaiService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiConfiguration;
import com.wordnik.swagger.annotations.Api;

@Api(value = "import", description = "REST resource for extracting data from sources and loading into HDFS")
@RestController
public class EaiResource {

    @Autowired
    private EaiService eaiService;

    @RequestMapping(value = "/extractAndImport", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    public AppSubmission eai(@RequestBody EaiConfiguration config) {
        AppSubmission submission = new AppSubmission(Arrays.<ApplicationId> asList(eaiService.invokeEai(config)));
        return submission;
    }

}
