package com.latticeengines.pls.controller.dcp;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.pls.service.dcp.SourceService;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "source")
@RestController
@RequestMapping("/source")
public class SourceResource {

    private static final Logger log = LoggerFactory.getLogger(SourceResource.class);

    @Inject
    private SourceService sourceService;

    @Inject
    private GraphDependencyToUIActionUtil graphDependencyToUIActionUtil;

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation("Create source")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    public Source createSource(@RequestBody SourceRequest sourceRequest) {
        try {
            return sourceService.createSource(sourceRequest);
        } catch (RuntimeException e) {
            log.error("Failed to create source: " + e.getMessage());
            UIAction action = graphDependencyToUIActionUtil.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, LedpCode.LEDP_60001);
        }
    }

    @GetMapping(value = "/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation("Get sources by sourceId")
    @PreAuthorize("hasRole('View_DCP_Projects')")
    public Source getSource(@PathVariable String sourceId) {
        return sourceService.getSource(sourceId);
    }

    @GetMapping(value = "/projectId/{projectId}")
    @ResponseBody
    @ApiOperation("Get sources by projectId")
    @PreAuthorize("hasRole('View_DCP_Projects')")
    public List<Source> getSourceUnderProduct(@PathVariable String productId) {
        return sourceService.getSourceList(productId);
    }

}
