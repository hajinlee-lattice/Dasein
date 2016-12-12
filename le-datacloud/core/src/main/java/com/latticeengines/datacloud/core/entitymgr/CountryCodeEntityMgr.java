package com.latticeengines.datacloud.core.entitymgr;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public interface CountryCodeEntityMgr {
    ConcurrentMap<String, String> findAll();

    String findByCountry(String country);

    Map<String, String> findAllCountries();
}
