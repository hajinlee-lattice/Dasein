package com.latticeengines.pls.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.TalkingPointAttribute;
import com.latticeengines.domain.exposed.cdl.TalkingPointNotionAttributes;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.TalkingPointsAttributesProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante/attributes",
        description = "REST resource for attributes related to Dante notions")
@RestController
@RequestMapping("/dante/attributes")
@PreAuthorize("hasRole('View_PLS_Plays')")
public class TalkingPointAttributesResource {

    private static final Logger log = LoggerFactory.getLogger(TalkingPointAttributesResource.class);

    @Autowired
    private TalkingPointsAttributesProxy talkingPointsAttributesProxy;

    @RequestMapping(value = "/accountattributes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get account attributes for this tenant")
    public List<TalkingPointAttribute> getAccountAttributes() {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return talkingPointsAttributesProxy.getAccountAttributes(tenant.getId());
    }

    @RequestMapping(value = "/recommendationattributes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get recommendation attributes")
    public List<TalkingPointAttribute> getRecommendationAttributes() {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return talkingPointsAttributesProxy.getRecommendationAttributes(tenant.getId());
    }

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Get attributes for given notions")
    public TalkingPointNotionAttributes getAttributesByNotions(@RequestBody List<String> notions) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return talkingPointsAttributesProxy.getAttributesByNotions(tenant.getId(), notions);
    }
}
