package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowOption;
import com.latticeengines.pls.dao.TargetMarketDataFlowOptionDao;

@Component
public class TargetMarketDataFlowOptionDaoImpl extends BaseDaoImpl<TargetMarketDataFlowOption> implements
        TargetMarketDataFlowOptionDao {
    @Override
    protected Class<TargetMarketDataFlowOption> getEntityClass() {
        return TargetMarketDataFlowOption.class;
    }
}
