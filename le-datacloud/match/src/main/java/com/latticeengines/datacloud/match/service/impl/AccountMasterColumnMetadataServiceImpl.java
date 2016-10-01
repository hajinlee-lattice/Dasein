package com.latticeengines.datacloud.match.service.impl;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;

@Component("accountMasterColumnMetadataService")
public class AccountMasterColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<AccountMasterColumn> {

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> accountmasterColumnService;

    @Override
    public boolean accept(String version) {
        return MatchTypeUtil.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    protected MetadataColumnService<AccountMasterColumn> getMetadataColumnService() {
        return accountmasterColumnService;
    }

}
