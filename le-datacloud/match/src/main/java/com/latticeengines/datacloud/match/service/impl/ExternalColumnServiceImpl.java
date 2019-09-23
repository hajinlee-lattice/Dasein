package com.latticeengines.datacloud.match.service.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;

@Component("externalColumnService")
public class ExternalColumnServiceImpl extends BaseMetadataColumnServiceImpl<ExternalColumn> {

    @Resource(name = "externalColumnEntityMgr")
    private MetadataColumnEntityMgr<ExternalColumn> externalColumnEntityMgr;

    @Value("${datacloud.match.latest.rts.cache.version:1.0.0}")
    private String latstRtsCache;

    private final ConcurrentMap<String, ConcurrentMap<String, ExternalColumn>> whiteColumnCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentSkipListSet<String>> blackColumnCache = new ConcurrentHashMap<>();

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForRTSBasedMatch(version);
    }

    @Override
    protected MetadataColumnEntityMgr<ExternalColumn> getMetadataColumnEntityMgr() {
        return externalColumnEntityMgr;
    }

    @Override
    protected ConcurrentMap<String, ConcurrentMap<String, ExternalColumn>> getWhiteColumnCache() {
        return whiteColumnCache;
    }

    @Override
    protected ConcurrentMap<String, ConcurrentSkipListSet<String>> getBlackColumnCache() {
        return blackColumnCache;
    }

    @Override
    protected String getLatestVersion() {
        return latstRtsCache;
    }

    @Override
    protected Class<ExternalColumn> getMetadataColumnClass() {
        return ExternalColumn.class;
    }

    @Override
    protected String getMDSTableName() {
        return ExternalColumn.TABLE_NAME;
    }
}
