package com.latticeengines.datacloudapi.api.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.network.exposed.propdata.InternalInterface;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.security.exposed.InternalResourceBase;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "internal", description = "Internal REST resource for propdata jobs.")
@RestController
@RequestMapping("/internal")
public class InternalResource extends InternalResourceBase implements InternalInterface {

    @Autowired
    private SqoopProxy sqoopProxy;

    @Override
    public AppSubmission importTable(SqoopImporter importer) {
        throw new UnsupportedOperationException("This is a place holder of a proxy method.");
    }

    @Override
    public AppSubmission exportTable(SqoopExporter exporter) {
        throw new UnsupportedOperationException("This is a place holder of a proxy method.");
    }

    @RequestMapping(value = "/sqoopimports", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Import data from SQL to HDFS")
    public AppSubmission importTable(@RequestBody SqoopImporter importer, HttpServletRequest request) {
        checkHeader(request);
        importer.setSync(false);
        importer.setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission());
        return sqoopProxy.importData(importer);
    }

    @RequestMapping(value = "/sqoopexports", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Export data from HDFS to SQL")
    public AppSubmission exportTable(@RequestBody SqoopExporter exporter, HttpServletRequest request) {
        checkHeader(request);
        exporter.setSync(false);
        exporter.setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission());
        return sqoopProxy.exportData(exporter);
    }

}
