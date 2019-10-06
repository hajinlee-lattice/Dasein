package com.latticeengines.metadata.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.metadata.service.DataUnitRuntimeService;

@Component("S3DataUnitService")
public class S3DataUnitService extends AbstractDataUnitRuntimeServiceImpl<S3DataUnit> //
        implements DataUnitRuntimeService {

    private static final Logger log = LoggerFactory.getLogger(S3DataUnitService.class);

    @Inject
    private S3Service s3Service;

    @Override
    protected Class<S3DataUnit> getUnitClz() {
        return S3DataUnit.class;
    }

    @Override
    public Boolean delete(DataUnit dataUnit) {
        S3DataUnit s3DataUnit = (S3DataUnit) dataUnit;
        String bucketName = s3DataUnit.getBucketName();
        if (StringUtils.isNotEmpty(bucketName)) {
            String prefix = s3DataUnit.getPrefix();
            s3Service.cleanupPrefix(bucketName, prefix);
        } else {
            S3PathBuilder.setS3Bucket(s3DataUnit);
            if (StringUtils.isNotEmpty(s3DataUnit.getBucketName())) {
                log.info(String.format("S3 data unit bucket name is %s, prefix is %s.", s3DataUnit.getBucketName(), s3DataUnit.getPrefix()));
                s3Service.cleanupPrefix(s3DataUnit.getBucketName(), s3DataUnit.getPrefix());
            }
        }
        return true;
    }

}
