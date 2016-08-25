package com.latticeengines.propdata.match.service.impl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.propdata.match.entitymanager.MetadataColumnEntityMgr;

@Component("externalColumnService")
public class ExternalColumnServiceImpl extends BaseMetadataColumnServiceImpl<ExternalColumn> {

    @Resource(name = "externalColumnEntityMgr")
    private MetadataColumnEntityMgr<ExternalColumn> externalColumnEntityMgr;

    private final ConcurrentMap<String, ExternalColumn> whiteColumnCache = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<String> blackColumnCache = new ConcurrentSkipListSet<>();

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

}
