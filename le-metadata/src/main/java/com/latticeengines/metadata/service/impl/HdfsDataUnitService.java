package com.latticeengines.metadata.service.impl;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.metadata.service.DataUnitRuntimeService;

@Component("hdfsDataUnitService")
public class HdfsDataUnitService extends AbstractDataUnitRuntimeServiceImpl<HdfsDataUnit> //
        implements DataUnitRuntimeService {

    private static final Logger log = LoggerFactory.getLogger(HdfsDataUnitService.class);

    @Inject
    protected Configuration yarnConfiguration;

    @Override
    protected Class<HdfsDataUnit> getUnitClz() {
        return HdfsDataUnit.class;
    }

    @Override
    public Boolean delete(DataUnit dataUnit) {
        try {
            HdfsDataUnit hdfsDataUnit = (HdfsDataUnit) dataUnit;
            log.info("deleting hdfs " + hdfsDataUnit.getName());
            HdfsUtils.rmdir(yarnConfiguration, hdfsDataUnit.getPath());
            log.info("deleted HdfsDataUnit record : tenant is " + hdfsDataUnit.getTenant() //
                    + ", name is " + hdfsDataUnit.getName());
            return true;
        } catch (Exception e) {
            log.error(e.getMessage());
            return false;
        }
    }

}
