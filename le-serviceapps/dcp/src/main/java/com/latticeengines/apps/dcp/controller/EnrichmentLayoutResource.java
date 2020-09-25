package com.latticeengines.apps.dcp.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.common.exposed.annotation.UseReaderConnection;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutOperationResult;
import com.latticeengines.domain.exposed.exception.LedpException;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "EnrichmentLayout")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/enrichmentlayout")
public class EnrichmentLayoutResource {

    @Inject
    private EnrichmentLayoutService enrichmentLayoutService;

    @GetMapping("/list")
    @ResponseBody
    @ApiOperation(value = "List Match Rule")
    @UseReaderConnection
    public List<EnrichmentLayout> getAll(@PathVariable String customerSpace) {
        return enrichmentLayoutService.getAll(customerSpace);
    }

    @PostMapping("/")
    @ResponseBody
    @ApiOperation(value = "Create a EnrichmentLayout")
    public ResponseDocument<EnrichmentLayoutOperationResult> create(@RequestBody EnrichmentLayout layout) {
        try {
            EnrichmentLayoutOperationResult result = enrichmentLayoutService.create(layout);
            return ResponseDocument.successResponse(result);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @PutMapping("/")
    @ResponseBody
    @ApiOperation(value = "Update EnrichmentLayout")
    public void updateEnrichmentLayout(@RequestBody EnrichmentLayout layout) {
        enrichmentLayoutService.update(layout);
    }

    @DeleteMapping("/{layoutId}")
    @ResponseBody
    @ApiOperation(value = "Delete enrichment layout by layoutId")
    public void deleteByLayoutId(@PathVariable String layoutId) {
        enrichmentLayoutService.delete(layoutId);
    }

    @DeleteMapping("/source/{sourceId}")
    @ResponseBody
    @ApiOperation(value = "Delete enrichment layout by sourceId")
    public void deleteBySourceId(@PathVariable String sourceId) {
        EnrichmentLayout enrichmentLayout = enrichmentLayoutService.findBySourceId(sourceId);
        enrichmentLayoutService.delete(enrichmentLayout);
    }
}
