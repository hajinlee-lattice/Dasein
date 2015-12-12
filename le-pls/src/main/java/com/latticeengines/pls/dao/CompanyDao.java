package com.latticeengines.pls.dao;

import java.util.Map;
import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.Company;

public interface CompanyDao extends BaseDao<Company> {

    Company findById(Long companyId);
    List<Company> findCompanies(Map<String, String> params);
    Long findCompanyCount(Map<String, String> params);
}
