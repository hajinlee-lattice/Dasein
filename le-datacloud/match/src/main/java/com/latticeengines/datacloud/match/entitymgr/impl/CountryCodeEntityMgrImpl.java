package com.latticeengines.datacloud.match.entitymgr.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public Map<String, String> findAll() {
        List<CountryCode> countryCodes = countryCodeDao.findAll();
        Map<String, String> countryCodeMap = new HashMap<String, String>();
        for (CountryCode code : countryCodes) {
            countryCodeMap.put(code.getCountryName(), code.getIsoCountryCode2Char());
        }
        return countryCodeMap;
    }
}
