package com.latticeengines.modelquality.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

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
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.modelquality.entitymgr.DataSetEntityMgr;

@Api(value = "modelquality", description = "REST resource to get DataSet parameters")
@RestController
public class DataSetResource {

    @Autowired
    private DataSetEntityMgr dataSetEntityMgr;

    private static final Log log = LogFactory.getLog(DataSetResource.class);

    @RequestMapping(value = "/datasets", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get DataSets")
    public ResponseDocument<List<DataSet>> getDataSets() {
        try {
            List<DataSet> dataSets = dataSetEntityMgr.findAll();
            return ResponseDocument.successResponse(dataSets);

        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/datasets", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upsert DataSets")
    public ResponseDocument<String> upsertDataSets(@RequestBody List<DataSet> datasets) {
        try {
            dataSetEntityMgr.deleteAll();
            dataSetEntityMgr.createDataSets(datasets);
            return ResponseDocument.successResponse("OK");
        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.successResponse("OK");
        }
    }
}
