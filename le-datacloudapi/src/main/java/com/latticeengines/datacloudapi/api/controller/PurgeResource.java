package com.latticeengines.datacloudapi.api.controller;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloudapi.engine.purge.service.PurgeService;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "purge", description = "REST resource for source purge")
@RestController
@RequestMapping("/purge")
public class PurgeResource {

    @Inject
    private PurgeService purgeService;

    @RequestMapping(value = "sources", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Get DataCloud sources to purge")
    public List<PurgeSource> getPurgeSources(
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod) {
        try {
            if (StringUtils.isEmpty(hdfsPod)) {
                hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
            }
            HdfsPodContext.changeHdfsPodId(hdfsPod);

            List<PurgeSource> list = purgeService.scan(hdfsPod, false);
            return list;
        } finally {
            hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
    }

    @RequestMapping(value = "sources/unknown", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Get unknown sources to purger")
    public List<String> getUnknownSources(
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod) {
        try {
            if (StringUtils.isEmpty(hdfsPod)) {
                hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
            }
            HdfsPodContext.changeHdfsPodId(hdfsPod);

            return purgeService.scanUnknownSources(hdfsPod);
        } finally {
            hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
    }
}
