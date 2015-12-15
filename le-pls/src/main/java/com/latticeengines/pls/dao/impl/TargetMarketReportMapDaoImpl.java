package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.TargetMarketReportMap;
import com.latticeengines.pls.dao.TargetMarketReportMapDao;

@Component("targetMarketReportMapDao")
public class TargetMarketReportMapDaoImpl extends BaseDaoImpl<TargetMarketReportMap> implements
        TargetMarketReportMapDao {
    @Override
    protected Class<TargetMarketReportMap> getEntityClass() {
        return TargetMarketReportMap.class;
    }
}
