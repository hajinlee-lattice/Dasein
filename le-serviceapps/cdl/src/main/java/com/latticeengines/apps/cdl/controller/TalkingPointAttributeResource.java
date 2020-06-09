package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.TalkingPointAttributeService;
import com.latticeengines.domain.exposed.cdl.TalkingPointAttribute;
import com.latticeengines.domain.exposed.cdl.TalkingPointNotionAttributes;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "talkingpoint attributes", description = "REST resource for talking point attributes")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/talkingpoints/attributes")
public class TalkingPointAttributeResource {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TalkingPointAttributeResource.class);

    @Inject
    private TalkingPointAttributeService talkingPointAttributeService;

    @GetMapping("/accountattributes")
    @ResponseBody
    @ApiOperation(value = "Get account attributes for this tenant")
    public List<TalkingPointAttribute> getAccountAttributes(@PathVariable String customerSpace) {
        return talkingPointAttributeService.getAccountAttributes();
    }

    @GetMapping("/recommendationattributes")
    @ResponseBody
    @ApiOperation(value = "Get recommendation attributes")
    public List<TalkingPointAttribute> getRecommendationAttributes(
            @PathVariable String customerSpace) {
        return talkingPointAttributeService.getRecommendationAttributes();
    }

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Get attributes for given notions")
    public TalkingPointNotionAttributes getAttributesByNotions(@PathVariable String customerSpace,
            @RequestBody List<String> notions) {
        return talkingPointAttributeService.getAttributesForNotions(notions);
    }
}
