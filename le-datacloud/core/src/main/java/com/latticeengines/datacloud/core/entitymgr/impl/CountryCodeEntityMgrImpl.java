package com.latticeengines.datacloud.core.entitymgr.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.core.dao.CountryCodeDao;
import com.latticeengines.datacloud.core.entitymgr.CountryCodeEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.CountryCode;

@Component("countryCodeEntityMgr")
public class CountryCodeEntityMgrImpl implements CountryCodeEntityMgr {

    @Resource(name = "countryCodeDao")
    private CountryCodeDao countryCodeDao;

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ConcurrentMap<String, String> findAllCountryCodes() {
        List<CountryCode> countryCodes = countryCodeDao.findAll();
        ConcurrentMap<String, String> countryCodeMap = new ConcurrentHashMap<>();
        for (CountryCode code : countryCodes) {
            countryCodeMap.put(code.getCountryName(), code.getIsoCountryCode2Char());
        }
        return countryCodeMap;
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public String findCountryCode(String country) {
        return countryCodeDao.findCountryCode(country);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public String findCountry(String country) {
        return countryCodeDao.findCountry(country);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Map<String, String> findAllCountries() {
        List<CountryCode> countryCodes = countryCodeDao.findAll();
        Map<String, String> countryMap = new HashMap<>();
        for (CountryCode code : countryCodes) {
            countryMap.put(code.getCountryName(), code.getIsoCountryName());
        }
        return countryMap;
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ConcurrentMap<String, String> findAllCountriesSync() {
        List<CountryCode> countryCodes = countryCodeDao.findAll();
        ConcurrentMap<String, String> countryMap = new ConcurrentHashMap<>();
        for (CountryCode code : countryCodes) {
            countryMap.put(code.getCountryName(), code.getIsoCountryName());
        }
        return countryMap;
    }
}
