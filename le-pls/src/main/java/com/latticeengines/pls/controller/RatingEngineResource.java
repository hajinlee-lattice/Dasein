package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.RatingCoverageService;
import com.latticeengines.pls.service.RatingEngineService;
import com.latticeengines.pls.service.RatingEntityPreviewService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ratingengine", description = "REST resource for rating engine")
@RestController
@RequestMapping("/ratingengines")
@PreAuthorize("hasRole('View_PLS_RatingEngines')")
public class RatingEngineResource {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineResource.class);

    @Autowired
    private RatingEngineService ratingEngineService;

    @Autowired
    private RatingCoverageService ratingCoverageService;

    @Autowired
    private RatingEntityPreviewService ratingEntityPreviewService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all Rating Engine summaries for a tenant")
    public List<RatingEngineSummary> getRatingEngineSummaries( //
            @RequestParam(value = "status", required = false) String status, //
            @RequestParam(value = "type", required = false) String type) {
        return ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(type, status);
    }

    @RequestMapping(value = "/types", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get types for Rating Engines")
    public List<RatingEngineType> getRatingEngineTypes() {
        return Arrays.asList(RatingEngineType.values());
    }

    @RequestMapping(value = "/{ratingEngineId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a Rating Engine given its id")
    public RatingEngine getRatingEngine( //
            @PathVariable String ratingEngineId, //
            HttpServletRequest request, //
            HttpServletResponse response) {
        return ratingEngineService.getRatingEngineById(ratingEngineId);
    }

    @RequestMapping(value = "/{ratingEngineId}/entitypreview", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get preview of Account and Contact as related to Rating Engine given its id")
    public DataPage getEntityPreview( //
            @PathVariable String ratingEngineId, //
            @RequestParam(value = "offset", required = true) long offset, //
            @RequestParam(value = "maximum", required = true) long maximum, //
            @RequestParam(value = "entityType", required = true) BusinessEntity entityType, //
            @RequestParam(value = "sortBy", required = false) String sortBy, //
            @RequestParam(value = "bucketFieldName", required = false) String bucketFieldName, //
            @RequestParam(value = "descending", required = false) Boolean descending, //
            @RequestParam(value = "lookupFieldNames", required = false) List<String> lookupFieldNames, //
            @RequestParam(value = "restrictNotNullSalesforceId", required = false) Boolean restrictNotNullSalesforceId, //
            @RequestParam(value = "freeFormTextSearch", required = false) String freeFormTextSearch) {
        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId);
        descending = descending == null ? false : descending;
        restrictNotNullSalesforceId = restrictNotNullSalesforceId == null ? false : restrictNotNullSalesforceId;
        return ratingEntityPreviewService.getEntityPreview(ratingEngine, offset, maximum, entityType, sortBy,
                descending, bucketFieldName, lookupFieldNames, restrictNotNullSalesforceId, freeFormTextSearch);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register or update a Rating Engine")
    @PreAuthorize("hasRole('Create_PLS_RatingEngines')")
    public RatingEngine createRatingEngine( //
            @RequestBody RatingEngine ratingEngine, //
            HttpServletRequest request) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            log.warn("Tenant is null for the request.");
            return null;
        }
        if (ratingEngine == null) {
            throw new NullPointerException("Rating Engine is null.");
        }
        return ratingEngineService.createOrUpdate(ratingEngine, tenant.getId());
    }

    @RequestMapping(value = "/{ratingEngineId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a Rating Engine given its id")
    @PreAuthorize("hasRole('Edit_PLS_RatingEngines')")
    public Boolean deleteRatingEngine( //
            @PathVariable String ratingEngineId, //
            HttpServletRequest request) {
        ratingEngineService.deleteById(ratingEngineId);
        return true;
    }

    @RequestMapping(value = "/coverage", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get CoverageInfo for ids in Rating count request")
    public RatingsCountResponse getRatingEngineCoverageInfo( //
            @RequestBody RatingsCountRequest ratingModelSegmentIds,
            @RequestParam(value = "getDummyCoverage", required = false) Boolean getDummyCoverage) {
        // TODO - anoop - this is only temporary, will remove it in next merge
        getDummyCoverage = getDummyCoverage == null ? false : getDummyCoverage;
        return ratingCoverageService.getCoverageInfo(ratingModelSegmentIds, getDummyCoverage);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Rating Models associated with a Rating Engine given its id")
    public Set<RatingModel> getRatingModels( //
            @PathVariable String ratingEngineId, //
            HttpServletRequest request, //
            HttpServletResponse response) {
        return ratingEngineService.getRatingModelsByRatingEngineId(ratingEngineId);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a particular Rating Model associated with a Rating Engine given its Rating Engine id and Rating Model id")
    public RatingModel getRatingModel( //
            @PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId, //
            HttpServletRequest request, //
            HttpServletResponse response) {
        return ratingEngineService.getRatingModel(ratingEngineId, ratingModelId);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a particular Rating Model associated with a Rating Engine given its Rating Engine id and Rating Model id")
    public RatingModel updateRatingModel( //
            @RequestBody RatingModel ratingModel, //
            @PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId, //
            HttpServletRequest request, HttpServletResponse response) {
        return ratingEngineService.updateRatingModel(ratingEngineId, ratingModelId, ratingModel);
    }

    @RequestMapping(value = "/{ratingEngineId}/plays", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Plays associated with a Rating Engine given its id")
    public Set<Play> getPlays(@PathVariable String ratingEngineId, //
            HttpServletRequest request, //
            HttpServletResponse response) {
        return createPlays();
    }

    private Set<Play> createPlays() {
        Set<Play> set = new HashSet<>();
        set.add(new Play());
        return set;
    }

}
