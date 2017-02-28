package com.latticeengines.datacloud.core.service;

import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public interface NameLocationService {

    void normalize(NameLocation nameLocation);

    void setDefaultCountry(NameLocation nameLocation);
}
