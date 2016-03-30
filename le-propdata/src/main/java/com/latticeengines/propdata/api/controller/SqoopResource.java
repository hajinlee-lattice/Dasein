package com.latticeengines.propdata.api.controller;

import javax.servlet.http.HttpServletRequest;

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
import com.latticeengines.network.exposed.propdata.SqoopInterface;
import com.latticeengines.propdata.core.service.SqlService;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

import edu.emory.mathcs.backport.java.util.Collections;

@Api(value = "sql jobs", description = "REST resource for propdata sql import and export jobs.")
@RestController
@RequestMapping("/sql")
public class SqoopResource extends InternalResourceBase implements SqoopInterface {

    @Autowired
    private SqlService sqlService;

    @Override
    public AppSubmission importTable(SqoopImporter importer) {
        throw new RuntimeException("This is a place holder of a proxy method.");
    }

    @Override
    public AppSubmission exportTable(SqoopExporter exporter) {
        throw new RuntimeException("This is a place holder of a proxy method.");
    }

    @SuppressWarnings("unchecked")
    @RequestMapping(value = "/imports", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Import data from SQL to HDFS")
    public AppSubmission importTable(@RequestBody SqoopImporter importer, HttpServletRequest request) {
        checkHeader(request);
        ApplicationId applicationId = sqlService.importTable(importer);
        return new AppSubmission(Collections.singletonList(applicationId));
    }

    @SuppressWarnings("unchecked")
    @RequestMapping(value = "/exports", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Export data from HDFS to SQL")
    public AppSubmission exportTable(@RequestBody SqoopExporter exporter, HttpServletRequest request) {
        checkHeader(request);
        ApplicationId applicationId = sqlService.exportTable(exporter);
        return new AppSubmission(Collections.singletonList(applicationId));
    }

}
