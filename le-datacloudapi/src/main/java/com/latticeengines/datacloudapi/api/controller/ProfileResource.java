package com.latticeengines.datacloudapi.api.controller;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_STAGE_SEGMENT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.statistics.ProfileArgument;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "profile resource")
@RestController
@RequestMapping("/profile")
public class ProfileResource {

    private static final Logger log = LoggerFactory.getLogger(ProfileResource.class);
    private static final String AM_PROFILE = "AMProfile";
    private static final String SOURCE_PROFILER = "SourceProfiler";
    private static final String KEY_PREFIX = DataCloudConstants.SERVICE_TENANT;
    private static final String PROFILE_ARGS = "ProfileArgs";

    @Inject
    private SourceAttributeEntityMgr srcAttrEntityMgr;

    @Inject
    private DataCloudVersionService dataCloudVersionService;

    private LocalCacheManager<String, Map<String, ProfileArgument>> amAttrsCache;

    @GetMapping("/arguments")
    @ResponseBody
    @ApiOperation(value = "Find profile arguments for customer data profiling")
    public Map<String, ProfileArgument> getProfileArgs( //
            @RequestParam(value = "dataCloudVersion", required = false) String dataCloudVersion) {
        initializeAMAttrsCache();
        if (StringUtils.isBlank(dataCloudVersion)) {
            dataCloudVersion = dataCloudVersionService.currentApprovedVersion().getVersion();
        }
        return amAttrsCache.getWatcherCache().get(KEY_PREFIX + "|" + PROFILE_ARGS + "|" + dataCloudVersion);
    }

    private void initializeAMAttrsCache() {
        if (amAttrsCache == null) {
            synchronized (this) {
                if (amAttrsCache == null) {
                    amAttrsCache = new LocalCacheManager<>(CacheName.DataCloudProfileCache, str -> {
                        String key = str.replace(KEY_PREFIX + "|", "");
                        return getAMAttrsFromDB(key);
                    }, 5, (int) (Math.random() * 30));
                    log.info("Initialized local cache DataCloudStatsCache.");
                }
            }
        }
    }

    private Map<String, ProfileArgument> getAMAttrsFromDB(String key) {
        String dataCloudVersion = key.split("\\|")[1];
        List<SourceAttribute> srcAttrs = srcAttrEntityMgr.getAttributes(AM_PROFILE,
                PROFILE_STAGE_SEGMENT, SOURCE_PROFILER, dataCloudVersion, true);
        log.info("Loaded {} source attributes from DB", srcAttrs.size());
        Map<String, ProfileArgument> amAttrsConfig = new HashMap<>();
        srcAttrs.forEach(attr -> amAttrsConfig.put(attr.getAttribute(),
                JsonUtils.deserialize(attr.getArguments(), ProfileArgument.class)));
        return amAttrsConfig;
    }

}
