package com.latticeengines.playmaker.entitymgr.impl;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.playmaker.dao.PlaymakerRecommendationDao;
import com.latticeengines.playmaker.dao.impl.PlaymakerRecommendationDaoImpl;
import com.latticeengines.playmaker.dao.impl.PlaymakerRecommendationDaoImplV740;

public class PlaymakerDaoFactoryImplUnitTestNG {

    @Test(groups = "unit")
    public void normalizedVer() {
        PlaymakerDaoFactoryImpl daoFactory = new PlaymakerDaoFactoryImpl();
        Assert.assertEquals(daoFactory.normalizedVer("7.4.0"), "007004000");
        Assert.assertEquals(daoFactory.normalizedVer("7.3.1"), "007003001");
        Assert.assertEquals(daoFactory.normalizedVer("7.3.2"), "007003002");
        Assert.assertEquals(daoFactory.normalizedVer("7.3.0"), "007003000");

    }

    @Test(groups = "unit")
    public void findDao() throws Exception {
        PlaymakerDaoFactoryImpl daoFactory = new PlaymakerDaoFactoryImpl();
        PlaymakerRecommendationDao defaultDao = new PlaymakerRecommendationDaoImpl(null);
        daoFactory.postConstruct();
        PlaymakerRecommendationDao dao = daoFactory.findDao(null, daoFactory.normalizedVer("7.4.0"), defaultDao);
        Assert.assertNotEquals(dao.getClass(), PlaymakerRecommendationDaoImpl.class);
        Assert.assertEquals(dao.getClass(), PlaymakerRecommendationDaoImplV740.class);

        dao = daoFactory.findDao(null, daoFactory.normalizedVer("7.5.2"), defaultDao);
        Assert.assertEquals(dao.getClass(), PlaymakerRecommendationDaoImplV740.class);

        dao = daoFactory.findDao(null, daoFactory.normalizedVer("7.4.01"), defaultDao);
        Assert.assertEquals(dao.getClass(), PlaymakerRecommendationDaoImplV740.class);

        dao = daoFactory.findDao(null, daoFactory.normalizedVer("7.4.0"), defaultDao);
        Assert.assertEquals(dao.getClass(), PlaymakerRecommendationDaoImplV740.class);

        dao = daoFactory.findDao(null, daoFactory.normalizedVer("7.3.1"), defaultDao);
        Assert.assertEquals(dao.getClass(), PlaymakerRecommendationDaoImpl.class);

        dao = daoFactory.findDao(null, daoFactory.normalizedVer("7.3.0"), defaultDao);
        Assert.assertEquals(dao.getClass(), PlaymakerRecommendationDaoImpl.class);

        dao = daoFactory.findDao(null, daoFactory.normalizedVer("7.1.6"), defaultDao);
        Assert.assertEquals(dao.getClass(), PlaymakerRecommendationDaoImpl.class);

    }
}
