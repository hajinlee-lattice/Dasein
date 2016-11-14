package com.latticeengines.sqoop.controller;

import java.util.Collections;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.sqoop.service.SqoopJobService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "jobs", description = "REST resource for sqoop jobs.")
@RestController
@RequestMapping("/jobs")
public class SqoopJobResource {

    @Autowired
    private SqoopJobService sqoopJobService;

    @RequestMapping(value = "/import", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Import data from SQL to HDFS")
    public AppSubmission importTable(@RequestBody SqoopImporter importer) {
        importer.setSync(false);
        ApplicationId applicationId = sqoopJobService.importData(importer);
        return new AppSubmission(Collections.singletonList(applicationId));
    }

    @RequestMapping(value = "/export", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Export data from HDFS to SQL")
    public AppSubmission exportTable(@RequestBody SqoopExporter exporter) {
        exporter.setSync(false);
        ApplicationId applicationId = sqoopJobService.exportData(exporter);
        return new AppSubmission(Collections.singletonList(applicationId));
    }

}
