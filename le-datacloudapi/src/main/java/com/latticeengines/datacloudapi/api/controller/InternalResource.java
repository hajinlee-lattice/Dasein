package com.latticeengines.datacloudapi.api.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "internal", description = "Internal REST resource for propdata jobs.")
@RestController
@RequestMapping("/internal")
public class InternalResource {

    @Inject
    private SqoopProxy sqoopProxy;

    @RequestMapping(value = "/sqoopimports", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Import data from SQL to HDFS")
    public AppSubmission importTable(@RequestBody SqoopImporter importer) {
        importer.setSync(false);
        importer.setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission());
        return sqoopProxy.importData(importer);
    }

    @RequestMapping(value = "/sqoopexports", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Export data from HDFS to SQL")
    public AppSubmission exportTable(@RequestBody SqoopExporter exporter) {
        exporter.setSync(false);
        exporter.setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission());
        return sqoopProxy.exportData(exporter);
    }

}
