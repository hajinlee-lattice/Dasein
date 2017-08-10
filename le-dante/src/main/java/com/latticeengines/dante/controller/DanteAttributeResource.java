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

import com.latticeengines.dante.service.DanteAttributeService;
import com.latticeengines.domain.exposed.dante.DanteAttribute;
import com.latticeengines.domain.exposed.dante.DanteNotionAttributes;
import com.latticeengines.network.exposed.dante.DanteAttributesInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante", description = "REST resource for attributes related to Dante notions")
@RestController
@RequestMapping("/attributes")
public class DanteAttributeResource implements DanteAttributesInterface {
    private static final Logger log = LoggerFactory.getLogger(DanteAttributeResource.class);

    @Autowired
    private DanteAttributeService danteAttributeService;

    @RequestMapping(value = "/accountattributes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get account attributes for this tenant")
    public List<DanteAttribute> getAccountAttributes(@RequestParam("customerSpace") String customerSpace) {
        return danteAttributeService.getAccountAttributes(customerSpace);
    }

    @RequestMapping(value = "/recommendationattributes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get recommendation attributes")
    public List<DanteAttribute> getRecommendationAttributes(@RequestParam("customerSpace") String customerSpace) {
        return danteAttributeService.getRecommendationAttributes(customerSpace);
    }

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Get attributes for given notions")
    public DanteNotionAttributes getAttributesByNotions(@RequestBody List<String> notions,
            @RequestParam("customerSpace") String customerSpace) {
        return danteAttributeService.getAttributesForNotions(notions, customerSpace);
    }
}
