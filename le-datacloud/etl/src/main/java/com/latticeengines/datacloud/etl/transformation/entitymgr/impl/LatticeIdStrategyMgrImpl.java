package com.latticeengines.datacloud.etl.transformation.entitymgr.impl;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.etl.transformation.dao.LatticeIdStrategyDao;
import com.latticeengines.datacloud.etl.transformation.entitymgr.LatticeIdStrategyEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.LatticeIdStrategy;

@Component("latticeIdStrategyMgr")
public class LatticeIdStrategyMgrImpl implements LatticeIdStrategyEntityMgr {
    @Autowired
    private LatticeIdStrategyDao latticeIdStrategyDao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public LatticeIdStrategy getStrategyByName(String name) {
        List<LatticeIdStrategy> list = latticeIdStrategyDao.getStrategyByName(name);
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }
        LatticeIdStrategy strategy = new LatticeIdStrategy();
        strategy.copyBasicInfo(list.get(0));
        for (LatticeIdStrategy obj : list) {
            strategy.addKeyMap(obj.getKeySet(), obj.getAttrs());
        }
        return strategy;
    }
}
