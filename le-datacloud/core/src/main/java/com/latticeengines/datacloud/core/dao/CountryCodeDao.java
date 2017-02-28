package com.latticeengines.datacloud.core.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.CountryCode;

public interface CountryCodeDao extends BaseDao<CountryCode> {
    String findCountryCode(String country);

    String findCountry(String country);
}
