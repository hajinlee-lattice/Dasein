package com.latticeengines.propdata.api.controller;

import java.util.Collections;
import java.util.Date;

import javax.servlet.http.HttpServletRequest;

import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.service.MetadataColumnService;
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
import com.latticeengines.domain.exposed.propdata.PropDataJobConfiguration;
import com.latticeengines.network.exposed.propdata.InternalInterface;
import com.latticeengines.propdata.core.service.SqoopService;
import com.latticeengines.propdata.match.service.InternalService;
import com.latticeengines.propdata.match.service.PropDataYarnService;
import com.latticeengines.security.exposed.InternalResourceBase;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "inernal", description = "Internal REST resource for propdata jobs.")
@RestController
@RequestMapping("/internal")
public class InternalResource extends InternalResourceBase implements InternalInterface {

    @Autowired
    private SqoopService sqoopService;

    @Autowired
    private PropDataYarnService yarnService;

    @Autowired
    private InternalService internalService;

    @Autowired
    private MetadataColumnService externalColumnService;

    @Override
    public AppSubmission importTable(SqoopImporter importer) {
        throw new UnsupportedOperationException("This is a place holder of a proxy method.");
    }

    @Override
    public AppSubmission exportTable(SqoopExporter exporter) {
        throw new UnsupportedOperationException("This is a place holder of a proxy method.");
    }

    @Override
    public AppSubmission submitYarnJob(PropDataJobConfiguration propDataJobConfiguration) {
        throw new UnsupportedOperationException("This is a place holder of a proxy method.");
    }

    @RequestMapping(value = "/sqoopimports", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Import data from SQL to HDFS")
    public AppSubmission importTable(@RequestBody SqoopImporter importer, HttpServletRequest request) {
        checkHeader(request);
        importer.setSync(false);
        ApplicationId applicationId = sqoopService.importTable(importer);
        return new AppSubmission(Collections.singletonList(applicationId));
    }

    @RequestMapping(value = "/sqoopexports", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Export data from HDFS to SQL")
    public AppSubmission exportTable(@RequestBody SqoopExporter exporter, HttpServletRequest request) {
        checkHeader(request);
        exporter.setSync(false);
        ApplicationId applicationId = sqoopService.exportTable(exporter);
        return new AppSubmission(Collections.singletonList(applicationId));
    }

    @RequestMapping(value = "/yarnjobs", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Match a block of input data in yarn container")
    public AppSubmission submitYarnJob(@RequestBody PropDataJobConfiguration jobConfiguration, HttpServletRequest request) {
        checkHeader(request);
        ApplicationId applicationId = yarnService.submitPropDataJob(jobConfiguration);
        return new AppSubmission(Collections.singletonList(applicationId));
    }

    @RequestMapping(value = "/externalcolumncache", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invalidate and rebuild caches in ExternalColumnService")
    public String invalidateExternalColumnsCache() {
        externalColumnService.loadCache();
        return "OK";
    }

    @RequestMapping(value = "/currentcachetableversion", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get version (creation timestamp) of DerivedColumnsCache table")
    @Override
    public Date currentCacheTableVersion() {
        return internalService.currentCacheTableCreatedTime();
    }


}
