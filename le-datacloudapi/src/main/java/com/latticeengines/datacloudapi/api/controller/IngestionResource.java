package com.latticeengines.datacloudapi.api.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
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

    @Inject
    private IngestionService ingestionService;

    @PostMapping
    @ResponseBody
    @ApiOperation(value = "Scan and trigger all ingestions that can proceed.")
    public List<IngestionProgress> scan(
            @RequestParam(value = "HdfsPod", required = false, defaultValue = "") String hdfsPod) {
        return ingestionService.scan(hdfsPod);
    }

    @PostMapping("/internal/{ingestionName}")
    @ResponseBody
    @ApiOperation(value = "Forcefully start an ingestion. "
            + "If an ingestion for same file/data is going on, skip the operation. "
            + "Only support for IngestionType: SFTP, SQL_TO_SOURCE, PATCH_BOOK")
    public IngestionProgress ingest(@PathVariable String ingestionName,
            @RequestBody IngestionRequest ingestionRequest,
            @RequestParam(value = "HdfsPod", required = false, defaultValue = "") String hdfsPod) {
        boolean immediate = Boolean.TRUE.equals(ingestionRequest.getStartNow());
        return ingestionService.ingest(ingestionName, ingestionRequest, hdfsPod, immediate);
    }
}
