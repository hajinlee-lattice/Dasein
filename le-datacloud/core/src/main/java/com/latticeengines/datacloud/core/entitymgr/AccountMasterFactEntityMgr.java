package com.latticeengines.datacloud.core.entitymgr;

import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFact;

public interface AccountMasterFactEntityMgr {

    AccountMasterFact findByDimensions(Long location, Long industry, Long numEmpRange, Long revRange, Long numLocRange,
                                       Long category);

}
