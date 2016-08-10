package com.latticeengines.propdata.match.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.propdata.match.LatticeAccount;
import com.latticeengines.propdata.match.entitymanager.AccountLookupEntryMgr;
import com.latticeengines.propdata.match.entitymanager.LatticeAccountMgr;


public interface AccountLookupService {

    List<LatticeAccount> batchLookup(AccountLookupRequest request);
    LatticeAccountMgr getAccountMgr(String version);
    AccountLookupEntryMgr getLookupMgr(String version);

}
