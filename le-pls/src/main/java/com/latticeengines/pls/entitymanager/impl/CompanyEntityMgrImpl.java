package com.latticeengines.pls.entitymanager.impl;

import java.util.Map;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.Company;
import com.latticeengines.pls.dao.CompanyDao;
import com.latticeengines.pls.entitymanager.CompanyEntityMgr;

@Component("companyEntityMgr")
public class CompanyEntityMgrImpl extends BaseEntityMgrImpl<Company> implements CompanyEntityMgr {

    @Autowired
    private CompanyDao companyDao;

    @Override
    public BaseDao<Company> getDao() {
        return companyDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Company findById(Long companyId) {
        return companyDao.findById(companyId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Company> findCompanies(Map<String, String> params) {
        return companyDao.findCompanies(params);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Long findCompanyCount(Map<String, String> params) {
        return companyDao.findCompanyCount(params);
    }
}
