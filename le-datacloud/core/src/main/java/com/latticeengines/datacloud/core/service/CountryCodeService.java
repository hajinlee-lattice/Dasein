package com.latticeengines.datacloud.core.service;

import java.io.Serializable;
import java.util.Map;

public interface CountryCodeService extends Serializable {
    String getCountryCode(String country);

    String getStandardCountry(String country);

    Map<String, String> getStandardCountries();
}
