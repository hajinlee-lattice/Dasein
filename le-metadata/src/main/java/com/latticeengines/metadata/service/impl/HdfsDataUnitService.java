package com.latticeengines.metadata.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.metadata.service.DataUnitRuntimeService;

@Component("HdfsDataUnitService")
public class HdfsDataUnitService extends DataUnitRuntimeService<HdfsDataUnit> {

    private static final Logger log = LoggerFactory.getLogger(HdfsDataUnitService.class);

    @Autowired
    protected Configuration yarnConfiguration;

    @Override
    public Boolean delete(HdfsDataUnit dataUnit) {
        try {
            log.info("delete hdfs " + dataUnit.getName());
            HdfsUtils.rmdir(yarnConfiguration, dataUnit.getPath());
            log.info("delete RedshiftDataUnit record : tenant is " + dataUnit.getTenant() + ", name is " + dataUnit.getName());
            return true;
        } catch (Exception e) {
            log.error(e.getMessage());
            return false;
        }
    }

    @Override
    public Boolean renameTableName(HdfsDataUnit dataUnit, String tablename) {
        throw new UnsupportedOperationException("HdfsDataUnitService can not support this method.");
    }
}
