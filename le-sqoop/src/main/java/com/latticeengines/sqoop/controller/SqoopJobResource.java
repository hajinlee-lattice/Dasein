package com.latticeengines.sqoop.controller;

import java.util.Collections;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.sqoop.exposed.service.SqoopJobService;
import com.latticeengines.sqoop.util.YarnConfigurationUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "jobs", description = "REST resource for sqoop jobs.")
@RestController
@RequestMapping("/jobs")
public class SqoopJobResource {

    @Inject
    private SqoopJobService sqoopJobService;

    @PostMapping(value = "/import")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Import data from SQL to HDFS")
    public AppSubmission importTable(@RequestBody SqoopImporter importer) {
        importer.setSync(false);
        ApplicationId applicationId = sqoopJobService.importData(importer, //
                YarnConfigurationUtils.getYarnConfiguration());
        return new AppSubmission(Collections.singletonList(applicationId));
    }

    @PostMapping(value = "/export")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Export data from HDFS to SQL")
    public AppSubmission exportTable(@RequestBody SqoopExporter exporter) {
        exporter.setSync(false);
        ApplicationId applicationId = sqoopJobService.exportData(exporter, //
                YarnConfigurationUtils.getYarnConfiguration());
        return new AppSubmission(Collections.singletonList(applicationId));
    }

}
