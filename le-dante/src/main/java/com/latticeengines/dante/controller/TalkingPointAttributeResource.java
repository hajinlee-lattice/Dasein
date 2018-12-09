package com.latticeengines.dante.controller;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
// import com.latticeengines.dante.service.TalkingPointAttributeService;
import com.latticeengines.domain.exposed.cdl.TalkingPointAttribute;
import com.latticeengines.domain.exposed.cdl.TalkingPointNotionAttributes;
// import com.latticeengines.network.exposed.dante.TalkingPointAttributesInterface;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante", description = "REST resource for attributes related to Dante notions")
@RestController
@RequestMapping("/attributes")
public class TalkingPointAttributeResource {// implements TalkingPointAttributesInterface {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TalkingPointAttributeResource.class);

    @Autowired
    // private TalkingPointAttributeService talkingPointAttributeService;

    @RequestMapping(value = "/accountattributes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get account attributes for this tenant")
    public List<TalkingPointAttribute> getAccountAttributes(
            @RequestParam("customerSpace") String customerSpace) {
        return null;
    }

    @RequestMapping(value = "/recommendationattributes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get recommendation attributes")
    public List<TalkingPointAttribute> getRecommendationAttributes(
            @RequestParam("customerSpace") String customerSpace) {
        return null;
    }

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Get attributes for given notions")
    public TalkingPointNotionAttributes getAttributesByNotions(@RequestBody List<String> notions,
            @RequestParam("customerSpace") String customerSpace) {
        return null;
    }
}
