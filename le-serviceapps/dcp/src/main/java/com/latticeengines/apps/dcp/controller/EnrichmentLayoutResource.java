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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.common.exposed.annotation.UseReaderConnection;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;
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
    public List<EnrichmentLayoutDetail> getAllLayout(@PathVariable String customerSpace,
                                                     @RequestParam(defaultValue = "0") int pageIndex,
                                                     @RequestParam(defaultValue = "20") int pageSize,
                                                     @RequestParam(defaultValue = "false") Boolean includeArchived) {
        return enrichmentLayoutService.getAll(customerSpace, includeArchived, pageIndex, pageSize);
    }

    @GetMapping("/layoutId/{layoutId}")
    @ResponseBody
    @ApiOperation(value = "Get Enrichment Layout by layoutId")
    public EnrichmentLayoutDetail getLayoutByLayoutId(@PathVariable String customerSpace, @PathVariable String layoutId) {
        return enrichmentLayoutService.findEnrichmentLayoutDetailByLayoutId(customerSpace, layoutId);
    }

    @GetMapping("/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation(value = "Get Enrichment Layout by sourceId")
    public EnrichmentLayoutDetail getLayoutBySourceId(@PathVariable String customerSpace, @PathVariable String sourceId) {
        return enrichmentLayoutService.findEnrichmentLayoutDetailBySourceId(customerSpace, sourceId);
    }

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Create a EnrichmentLayout")
    public ResponseDocument<EnrichmentLayoutOperationResult> create(@PathVariable String customerSpace, @RequestBody EnrichmentLayout layout) {
        try {
            EnrichmentLayoutOperationResult result = enrichmentLayoutService.create(customerSpace, layout);
            return ResponseDocument.successResponse(result);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @PutMapping("")
    @ResponseBody
    @ApiOperation(value = "Update EnrichmentLayout")
    public EnrichmentLayoutDetail updateEnrichmentLayout(@PathVariable String customerSpace, @RequestBody EnrichmentLayout layout) {
        enrichmentLayoutService.update(customerSpace, layout);
        return new EnrichmentLayoutDetail(layout);
    }

    /**
     * Set the deleted flag to true.
     * @param customerSpace
     * @param layoutId
     */
    @DeleteMapping("/layoutId/{layoutId}")
    @ResponseBody
    @ApiOperation(value = "Delete enrichment layout by layoutId")
    public void deleteByLayoutId(@PathVariable String customerSpace, @PathVariable String layoutId) {
        enrichmentLayoutService.deleteLayoutByLayoutId(customerSpace, layoutId);
    }

    /**
     * Set the deleted flag to true.
     * @param customerSpace
     * @param sourceId
     */
    @DeleteMapping("/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation(value = "Delete enrichment layout by sourceId")
    public void deleteBySourceId(@PathVariable String customerSpace, @PathVariable String sourceId) {
        enrichmentLayoutService.deleteLayoutByLayoutId(customerSpace, sourceId);
    }
}
