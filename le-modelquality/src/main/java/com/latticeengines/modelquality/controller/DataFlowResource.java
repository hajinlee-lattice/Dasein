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
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.modelquality.entitymgr.DataFlowEntityMgr;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource to get DataFlow parameters")
@RestController
public class DataFlowResource {
    
    @Autowired
    private DataFlowEntityMgr dataFlowEntityMgr;

    private static final Log log = LogFactory.getLog(DataFlowResource.class);

    @RequestMapping(value = "/dataflows", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get DataFlows")
    public ResponseDocument<List<DataFlow>> getFlows() {
        try {
            List<DataFlow> dataFlows = dataFlowEntityMgr.findAll();
            return ResponseDocument.successResponse(dataFlows);
            
        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }
    
    @RequestMapping(value = "/dataflows", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upsert DataFlows")
    public ResponseDocument<String> upsertDataFlows(@RequestBody List<DataFlow> dataflows) {
        try {
            dataFlowEntityMgr.deleteAll();
            dataFlowEntityMgr.createDataFlows(dataflows);
            return ResponseDocument.successResponse("OK");
        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }

}
