package com.latticeengines.datacloud.match.service;

import java.io.Serializable;

public interface CountryCodeService extends Serializable {
    String getCountryCode(String standardizedCountry);
}
