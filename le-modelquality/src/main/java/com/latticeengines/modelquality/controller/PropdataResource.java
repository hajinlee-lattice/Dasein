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
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.entitymgr.PropDataEntityMgr;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource to get PropData parameters")
@RestController
public class PropdataResource {

    @Autowired
    private PropDataEntityMgr propDataEntityMgr;

    private static final Log log = LogFactory.getLog(PropdataResource.class);

    @RequestMapping(value = "/propdataconfigs", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get PropDatas")
    public ResponseDocument<List<PropData>> getPropDatas() {
        try {
            List<PropData> propdatas = propDataEntityMgr.findAll();
            return ResponseDocument.successResponse(propdatas);

        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/propdataconfigs", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upsert PropDatas")
    public ResponseDocument<String> upsertPropDatas(@RequestBody List<PropData> propDatas) {
        try {
            propDataEntityMgr.deleteAll();
            propDataEntityMgr.createPropDatas(propDatas);
            return ResponseDocument.successResponse("OK");
        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }
}
