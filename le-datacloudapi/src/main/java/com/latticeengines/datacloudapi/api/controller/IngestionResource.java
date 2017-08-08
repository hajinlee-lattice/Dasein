package com.latticeengines.datacloudapi.api.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloudapi.engine.ingestion.service.IngestionService;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ingestion", description = "REST resource for source ingestion")
@RestController
@RequestMapping("/ingestions")
public class IngestionResource {

    @Autowired
    private IngestionService ingestionService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Scan and trigger all ingestions that can proceed.")
    public List<IngestionProgress> scan(
            @RequestParam(value = "HdfsPod", required = false, defaultValue = "") String hdfsPod) {
        return ingestionService.scan(hdfsPod);
    }

    @RequestMapping(value = "/internal/{ingestionName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Forcefully start an ingestion. "
            + "If an ingestion for same file/data is going on, skip the operation. "
            + "Only support for IngestionType: SFTP, SQL_TO_SOURCE")
    public IngestionProgress ingest(@PathVariable String ingestionName,
            @RequestBody IngestionRequest ingestionRequest,
            @RequestParam(value = "HdfsPod", required = false, defaultValue = "") String hdfsPod) {
        return ingestionService.ingest(ingestionName, ingestionRequest, hdfsPod);
    }
}
