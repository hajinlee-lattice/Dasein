package com.latticeengines.datacloudapi.api.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.network.exposed.propdata.PublicationInterface;
import com.latticeengines.datacloudapi.engine.publication.service.PublicationService;
import com.latticeengines.security.exposed.InternalResourceBase;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "publication", description = "REST resource for source publication")
@RestController
@RequestMapping("/publications")
public class PublicationResource extends InternalResourceBase implements PublicationInterface {

    @Autowired
    private PublicationService publicationService;

    @Override
    public List<PublicationProgress> scan(String hdfsPod) {
        throw new UnsupportedOperationException("This is a place holder of a proxy method.");
    }

    @Override
    public PublicationProgress publish(String publicationName, PublicationRequest publicationRequest, String hdfsPod) {
        throw new UnsupportedOperationException("This is a place holder of a proxy method.");
    }

    @RequestMapping(value = "/", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Scan all publication progresses that can be proceeded. "
            + "url parameter podid is for testing purpose.")
    public List<PublicationProgress> scan(
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod,
            HttpServletRequest request) {
        checkHeader(request);
        try {
            return publicationService.scan(hdfsPod);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25009, e);
        }
    }

    @RequestMapping(value = "internal/{publicationName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Forcefully trigger a new publication for a source at its latest version. "
            + "If a publication with the same source version already exists, skip operation. "
            + "url parameter podid is for testing purpose.")
    public PublicationProgress publish(@PathVariable String publicationName,
            @RequestBody PublicationRequest publicationRequest,
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod,
            HttpServletRequest request) {
        checkHeader(request);
        try {
            return publicationService.publish(publicationName, publicationRequest, hdfsPod);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25009, e);
        }
    }

}
