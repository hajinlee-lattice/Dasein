package com.latticeengines.pls.controller.dcp;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.service.dcp.EnrichmentLayoutService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "EnrichmentLayout")
@RestController
@RequestMapping("/enrichment-layouts")
public class EnrichmentLayoutResource {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentLayoutResource.class);

    @Inject
    private EnrichmentLayoutService enrichmentLayoutService;

    @PostMapping
    @ResponseBody
    @ApiOperation("Create new enrichment layout")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    public ResponseDocument<String> create(@RequestBody EnrichmentLayout enrichmentLayout) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return enrichmentLayoutService.create(customerSpace.toString(), enrichmentLayout);
    }

    @GetMapping("/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation("Get an EnrichmentLayout by sourceId")
    public EnrichmentLayoutDetail getEnrichmentLayoutBySourceId(@PathVariable String sourceId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        checkCustomerSpace(customerSpace);
        return sourceId != null
                ? enrichmentLayoutService.getEnrichmentLayoutBySourceId(customerSpace.toString(), sourceId)
                : null;
    }

    @GetMapping("/layoutId/{layoutId}")
    @ResponseBody
    @ApiOperation("Get an EnrichmentLayout by layoutId")
    public EnrichmentLayoutDetail getEnrichmentLayoutByLayoutId(@PathVariable String layoutId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        checkCustomerSpace(customerSpace);
        return layoutId != null
                ? enrichmentLayoutService.getEnrichmentLayoutByLayoutId(customerSpace.toString(), layoutId)
                : null;
    }

    @PutMapping
    @ResponseBody
    @ApiOperation("Update a EnrichmentLayout")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    public ResponseDocument<String> update(@RequestBody EnrichmentLayout enrichmentLayout) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return enrichmentLayoutService.update(customerSpace.toString(), enrichmentLayout);
    }

    @DeleteMapping("/layoutId/{layoutId}")
    @ResponseBody
    @ApiOperation("Delete a EnrichmentLayout")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    public void delete(@PathVariable String layoutId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        enrichmentLayoutService.delete(customerSpace.toString(), layoutId);
    }

    private void checkCustomerSpace(CustomerSpace customerSpace) {
        if (null == customerSpace) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
    }
}
