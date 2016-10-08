package com.latticeengines.datacloud.match.service.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;

@Component("externalColumnService")
public class ExternalColumnServiceImpl extends BaseMetadataColumnServiceImpl<ExternalColumn> {

    @Resource(name = "externalColumnEntityMgr")
    private MetadataColumnEntityMgr<ExternalColumn> externalColumnEntityMgr;

    @Value("${datacloud.match.latest.rts.cache.version:1.0.0}")
    private String latstRtsCache;

    private final ConcurrentMap<String, ExternalColumn> whiteColumnCache = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<String> blackColumnCache = new ConcurrentSkipListSet<>();

    @Override
    public boolean accept(String version) {
        return MatchTypeUtil.isValidForRTSBasedMatch(version);
    }

    @Override
    protected MetadataColumnEntityMgr<ExternalColumn> getMetadataColumnEntityMgr() {
        return externalColumnEntityMgr;
    }

    @Override
    protected ConcurrentMap<String, ExternalColumn> getWhiteColumnCache() {
        return whiteColumnCache;
    }

    @Override
    protected ConcurrentSkipListSet<String> getBlackColumnCache() {
        return blackColumnCache;
    }

    @Override
    protected boolean isLatestVersion(String dataCloudVersion) {
        return true;
    }

    @Override
    protected String getLatestVersion() {
        return latstRtsCache;
    }
}
