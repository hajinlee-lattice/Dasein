package com.latticeengines.apps.dcp.controller;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.common.exposed.annotation.UseReaderConnection;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "EnrichmentLayout")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/enrichmentlayout")
public class EnrichmentLayoutResource {

    @Inject
    private EnrichmentLayoutService enrichmentLayoutService;

    @GetMapping("/")
    @ResponseBody
    @ApiOperation(value = "List Match Rule")
    @UseReaderConnection
    public List<EnrichmentLayout> getAll(@PathVariable String customerSpace) {
        return Collections.emptyList();
    }
}
