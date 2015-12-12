package com.latticeengines.pls.entitymanager;

import java.util.Map;
import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.Company;

public interface CompanyEntityMgr extends BaseEntityMgr<Company> {
    Company findById(Long companyID);
    List<Company> findCompanies(Map<String, String> params);
    Long findCompanyCount(Map<String, String> params);
}
