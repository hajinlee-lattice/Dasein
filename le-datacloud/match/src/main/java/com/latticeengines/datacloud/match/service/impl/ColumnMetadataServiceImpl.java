package com.latticeengines.datacloud.match.service.impl;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;

@Component("columnMetadataService")
public class ColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<ExternalColumn> {

    @Resource(name = "externalColumnService")
    private MetadataColumnService<ExternalColumn> externalColumnService;

    @Value("${datacloud.match.latest.rts.cache.version:1.0.0}")
    private String latstRtsCache;

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForRTSBasedMatch(version);
    }

    @Override
    protected MetadataColumnService<ExternalColumn> getMetadataColumnService() {
        return externalColumnService;
    }

    @Override
    protected boolean isLatestVersion(String dataCloudVersion) {
        return true;
    }

    @Override
    protected String getLatestVersion() {
        return latstRtsCache;
    }

    @Override
    protected boolean refreshCacheNeeded() {
        return true;
    }

    @Override
    public AttributeRepository getAttrRepo(String dataCloudVersion) {
        throw new UnsupportedOperationException("Attribute repository is not supported in 1.0.0");
    }
}
