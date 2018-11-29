package com.latticeengines.apps.cdl.controller;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import com.latticeengines.apps.cdl.service.TalkingPointAttributeService;
import com.latticeengines.domain.exposed.cdl.TalkingPointAttribute;
import com.latticeengines.domain.exposed.cdl.TalkingPointNotionAttributes;
// import com.latticeengines.network.exposed.dante.TalkingPointAttributesInterface;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "talkingpoint attributes", description = "REST resource for talking point attributes")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/talkingpoints/attributes")
public class TalkingPointAttributeResource {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TalkingPointAttributeResource.class);

    @Autowired
    private TalkingPointAttributeService talkingPointAttributeService;

    @RequestMapping(value = "/accountattributes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get account attributes for this tenant")
    public List<TalkingPointAttribute> getAccountAttributes(@PathVariable String customerSpace) {
        return talkingPointAttributeService.getAccountAttributes(customerSpace);
    }

    @RequestMapping(value = "/recommendationattributes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get recommendation attributes")
    public List<TalkingPointAttribute> getRecommendationAttributes(
            @PathVariable String customerSpace) {
        return talkingPointAttributeService.getRecommendationAttributes(customerSpace);
    }

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Get attributes for given notions")
    public TalkingPointNotionAttributes getAttributesByNotions(@PathVariable String customerSpace,
            @RequestBody List<String> notions) {
        return talkingPointAttributeService.getAttributesForNotions(notions, customerSpace);
    }
}
