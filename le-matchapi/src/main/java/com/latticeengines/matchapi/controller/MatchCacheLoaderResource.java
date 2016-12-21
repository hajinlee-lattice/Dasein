package com.latticeengines.matchapi.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.matchapi.service.CacheLoaderConfig;
import com.latticeengines.matchapi.service.impl.BaseCacheLoaderService;

@Api(value = "cacheloader", description = "Cache Loader REST APIs to load cache for match")
@RestController
@RequestMapping("/cacheloader")
public class MatchCacheLoaderResource {
    private static final Log log = LogFactory.getLog(MatchCacheLoaderResource.class);

    @Autowired
    private BaseCacheLoaderService<GenericRecord> cacheLoaderService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "")
    private void loadCache(@RequestBody CacheLoaderConfig config) {
        cacheLoaderService.loadCache(config);
    }

}
