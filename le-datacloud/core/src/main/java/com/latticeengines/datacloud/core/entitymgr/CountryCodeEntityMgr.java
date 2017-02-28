package com.latticeengines.datacloud.core.entitymgr;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public interface CountryCodeEntityMgr {
    ConcurrentMap<String, String> findAllCountryCodes();

    String findCountryCode(String country);

    String findCountry(String country);

    Map<String, String> findAllCountries();

    ConcurrentMap<String, String> findAllCountriesSync();
}
