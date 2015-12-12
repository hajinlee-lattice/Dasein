package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.AmAttribute;
import com.latticeengines.pls.entitymanager.AmAttributeEntityMgr;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "amattibute", description = "REST resource for account master attributes")
@RestController
@RequestMapping("/amattributes")
// @PreAuthorize("hasRole('View_PLS_Data')")
public class AmAttributeResource {

    private static final Log log = LogFactory.getLog(AmAttributeResource.class);

    @Autowired
    private AmAttributeEntityMgr amAttributeEntityMgr;

    @Autowired
    private SessionService sessionService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of companies which meet select criterias")
    public List<AmAttribute> getAttributes(@RequestParam Map<String,String> reqParams) {
        String key = reqParams.get("AttrKey");
        String parentKey = reqParams.get("ParentKey");
        String parentValue = reqParams.get("ParentValue");
        log.info("search attributes Key=" + key + " pKey=" + parentKey + " pValue=" + parentValue );
        
        return amAttributeEntityMgr.findAttributes(key, parentKey, parentValue);
    }
}
