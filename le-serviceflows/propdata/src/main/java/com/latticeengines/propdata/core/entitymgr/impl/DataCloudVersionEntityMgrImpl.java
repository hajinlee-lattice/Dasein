package com.latticeengines.propdata.core.entitymgr.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.propdata.core.dao.DataCloudVersionDao;
import com.latticeengines.propdata.core.entitymgr.DataCloudVersionEntityMgr;

@Component("dataCloudVersionEntityMgr")
public class DataCloudVersionEntityMgrImpl implements DataCloudVersionEntityMgr {

    private static final Log log = LogFactory.getLog(DataCloudVersionEntityMgrImpl.class);

    @Autowired
    private DataCloudVersionDao dataCloudVersionDao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public DataCloudVersion latestApprovedForMajorVersion(String version) {
        String majorVersion = parseMajorVersion(version);
        DataCloudVersion dataCloudVersion = dataCloudVersionDao
                .latestApprovedForMajorVersion(majorVersion);
        if (dataCloudVersion == null) {
            log.warn("Cannot find any approved version for major version " + majorVersion);
        }
        return dataCloudVersion;
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public DataCloudVersion findVersion(String version) {
        DataCloudVersion dataCloudVersion = dataCloudVersionDao.findByField("Version", version);
        if (dataCloudVersion == null) {
            log.info("Cannot find data cloud version to be deleted " + version);
        }
        return dataCloudVersion;
    }


    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public DataCloudVersion createVersion(DataCloudVersion version) {
        dataCloudVersionDao.create(version);
        return findVersion(version.getVersion());
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public void deleteVersion(String version) {
        DataCloudVersion versionInDb = findVersion(version);
        if (versionInDb != null) {
            dataCloudVersionDao.delete(versionInDb);
        }
    }

    private String parseMajorVersion(String version) {
        String[] tokens = version.split("\\.");
        if (tokens.length < 2) {
            throw new RuntimeException("Cannot parse a major version from " + version);
        }
        return tokens[0] + "." + tokens[1];
    }

}
