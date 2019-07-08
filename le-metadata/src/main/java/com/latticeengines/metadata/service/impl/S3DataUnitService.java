package com.latticeengines.metadata.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
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
        String linkDir = s3DataUnit.getLinkedDir();
        log.info("LinkDir is " + linkDir);
        linkDir = linkDir.replaceAll("s3a://", "");
        String bucketName = linkDir.substring(0, linkDir.indexOf('/'));
        log.info("bucketName is " + bucketName);
        String prefix = linkDir.substring(linkDir.indexOf('/') + 1);
        log.info("prefix is " + prefix);
        s3Service.cleanupPrefix(bucketName, prefix);
        return true;
    }

}
