package com.latticeengines.datacloud.match.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.CountryCode;

public interface CountryCodeDao extends BaseDao<CountryCode> {
    String findByCountry(String country);
}
