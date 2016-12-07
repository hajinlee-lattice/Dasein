package com.latticeengines.datacloud.core.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFact;

public interface AccountMasterFactDao extends BaseDao<AccountMasterFact> {

    AccountMasterFact findByDimensions(Long location, Long industry, Long numEmpRange, Long revRange, Long numLocRange,
            Long category);

}
