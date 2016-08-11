package com.latticeengines.propdata.match.service.impl;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.match.BulkMatchInput;
import com.latticeengines.domain.exposed.propdata.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.MatchExecutor;
import com.latticeengines.propdata.match.service.MatchPlanner;

@Component("realTimeMatchWithAccountMasterService")
public class RealTimeMatchWithAccountMasterServiceImpl extends RealTimeMatchWithDerivedColumnCacheServiceImpl {

    private static final String DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING = "2.";

    @Autowired
    @Qualifier("realTimeMatchPlanner")
    private MatchPlanner matchPlanner;

    @Autowired
    @Qualifier("realTimeMatchExecutor")
    private MatchExecutor matchExecutor;

    @Override
    public boolean accept(String version) {
        if (!StringUtils.isEmpty(version)
                && version.trim().startsWith(DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING)) {
            return true;
        }

        return false;
    }

    @Override
    @MatchStep(threshold = 0L)
    public MatchOutput match(MatchInput input) {
        // TODO - need to be implemented by Anoop in next txn
        throw new NotImplementedException("Impl of account master based match is yet to be implemented");
    }

    @Override
    @MatchStep(threshold = 0L)
    public BulkMatchOutput matchBulk(BulkMatchInput input) {
        // TODO - need to be implemented by Anoop in next txn
        throw new NotImplementedException("Impl of account master based match is yet to be implemented");
    }
}
