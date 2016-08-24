package com.latticeengines.propdata.match.service.impl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.propdata.manage.Predefined;
import com.latticeengines.propdata.match.entitymanager.MetadataColumnEntityMgr;

@Component("accountMasterColumnService")
public class AccountMasterColumnServiceImpl extends BaseMetadataColumnServiceImpl<AccountMasterColumn> {

    @Resource(name = "accountMasterColumnEntityMgr")
    private MetadataColumnEntityMgr<AccountMasterColumn> metadataColumnEntityMgr;

    private final ConcurrentMap<String, AccountMasterColumn> whiteColumnCache = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<String> blackColumnCache = new ConcurrentSkipListSet<>();

    @Override
    protected MetadataColumnEntityMgr<AccountMasterColumn> getMetadataColumnEntityMgr() {
        return metadataColumnEntityMgr;
    }

    @Override
    protected ConcurrentMap<String, AccountMasterColumn> getWhiteColumnCache() {
        return whiteColumnCache;
    }

    @Override
    protected ConcurrentSkipListSet<String> getBlackColumnCache() {
        return blackColumnCache;
    }

    @Override
    public List<AccountMasterColumn> findByColumnSelection(Predefined selectName) {
        return getMetadataColumnEntityMgr().findByTag(selectName.getName());
    }
}
