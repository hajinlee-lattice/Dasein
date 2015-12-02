package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.TargetMarket;

public interface TargetMarketDao extends BaseDao<TargetMarket> {

    /**
     * @param name
     *    : the name of target market to query against
     * @return target market returned, null if none.
     */
    TargetMarket findTargetMarketByName(String name);

    /**
     * @param name
     *    : the name of target market we want to delete
     * @return true if successfully delete a target market; false if none is
     *    deleted
     */
    boolean deleteTargetMarketByName(String name);

    TargetMarket findDefaultTargetMarket();
}
