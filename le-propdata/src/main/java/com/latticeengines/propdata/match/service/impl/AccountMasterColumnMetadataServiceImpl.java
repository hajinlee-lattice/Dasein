package com.latticeengines.propdata.match.service.impl;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.AccountMasterColumn;
import com.latticeengines.propdata.match.service.MetadataColumnService;

@Component("accountMasterColumnMetadataService")
public class AccountMasterColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<AccountMasterColumn> {

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> accountmasterColumnService;

    @Override
    protected MetadataColumnService<AccountMasterColumn> getMetadataColumnService() {
        return accountmasterColumnService;
    }

}
