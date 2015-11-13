package com.latticeengines.playmaker.entitymgr.impl;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.playmaker.dao.PlaymakerDBVersionDao;
import com.latticeengines.playmaker.dao.PlaymakerRecommendationDao;
import com.latticeengines.playmaker.dao.impl.PlaymakerDBVersionDaoImpl;
import com.latticeengines.playmaker.dao.impl.PlaymakerRecommendationDaoImpl;
import com.latticeengines.playmaker.dao.impl.PlaymakerRecommendationDaoImplV740;
import com.latticeengines.playmaker.entitymgr.PlaymakerDaoFactory;

@Component("daoFactory")
public class PlaymakerDaoFactoryImpl implements PlaymakerDaoFactory {

    private static final Log log = LogFactory.getLog(PlaymakerDaoFactoryImpl.class);

    @Autowired
    private JdbcTemplateFactoryImpl templateFactory;

    private Map<String, Class<? extends PlaymakerRecommendationDao>> versionDaoMap;

    @PostConstruct
    public void postConstruct() {
        versionDaoMap = new TreeMap<>(Collections.reverseOrder());
        versionDaoMap.put(normalizedVer("7.4.0"), PlaymakerRecommendationDaoImplV740.class);
        versionDaoMap.put(normalizedVer("7.5.0"), PlaymakerRecommendationDaoImplV740.class);
        versionDaoMap.put(normalizedVer("7.6.0"), PlaymakerRecommendationDaoImplV740.class);
    }

    @Override
    public PlaymakerRecommendationDao getRecommendationDao(String tenantName) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PlaymakerRecommendationDao defaultDao = new PlaymakerRecommendationDaoImpl(namedJdbcTemplate);

        try {
            PlaymakerDBVersionDao versionDao = new PlaymakerDBVersionDaoImpl(namedJdbcTemplate);
            String version = versionDao.getDBVersion();
            String normalizedVer = normalizedVer(version);

            return findDao(namedJdbcTemplate, normalizedVer, defaultDao);

        } catch (Exception ex) {
            log.warn("Failed to get Dao!", ex);
        }
        return defaultDao;
    }

    PlaymakerRecommendationDao findDao(NamedParameterJdbcTemplate namedJdbcTemplate, String normalizedVer,
            PlaymakerRecommendationDao defaultDao) throws Exception {
        for (Map.Entry<String, Class<? extends PlaymakerRecommendationDao>> entry : versionDaoMap.entrySet()) {
            int result = normalizedVer.compareTo(entry.getKey());
            if (result >= 0) {
                Class<?> daoClass = entry.getValue();
                return (PlaymakerRecommendationDao) daoClass.getConstructor(NamedParameterJdbcTemplate.class)
                        .newInstance(namedJdbcTemplate);
            }
        }
        return defaultDao;
    }

    String normalizedVer(String version) {
        StringBuilder builder = new StringBuilder();
        String[] tokens = version.split("[.]");
        for (String token : tokens) {
            builder.append(StringUtils.leftPad(token, 3, '0'));
        }

        return builder.toString();
    }

}
