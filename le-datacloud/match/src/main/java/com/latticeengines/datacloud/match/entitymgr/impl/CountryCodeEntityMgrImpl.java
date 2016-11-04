package com.latticeengines.datacloud.match.entitymgr.impl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.match.dao.CountryCodeDao;
import com.latticeengines.datacloud.match.entitymgr.CountryCodeEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.CountryCode;

@Component("countryCodeEntityMgr")
public class CountryCodeEntityMgrImpl implements CountryCodeEntityMgr {

    @Resource(name = "countryCodeDao")
    private CountryCodeDao countryCodeDao;

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ConcurrentMap<String, String> findAll() {
        List<CountryCode> countryCodes = countryCodeDao.findAll();
        ConcurrentMap<String, String> countryCodeMap = new ConcurrentHashMap<String, String>();
        for (CountryCode code : countryCodes) {
            countryCodeMap.put(code.getCountryName(), code.getIsoCountryCode2Char());
        }
        return countryCodeMap;
    }
}
