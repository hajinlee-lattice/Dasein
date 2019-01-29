package com.latticeengines.metadata.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.metadata.service.DataUnitRuntimeService;

@Component("S3DataUnitService")
public class S3DataUnitService extends DataUnitRuntimeService<S3DataUnit> {

    private static final Logger log = LoggerFactory.getLogger(S3DataUnitService.class);

    @Inject
    private S3Service s3Service;

    @Override
    public Boolean delete(S3DataUnit dataUnit) {
        String linkDir = dataUnit.getLinkedDir();
        log.info("LinkDir is " + linkDir);
        linkDir = linkDir.replaceAll("s3a://", "");
        String bucketName = linkDir.substring(0, linkDir.indexOf('/'));
        log.info("bucketName is " + bucketName);
        String prefix = linkDir.substring(linkDir.indexOf('/')+1);
        log.info("prefix is " + prefix);
        s3Service.cleanupPrefix(bucketName, prefix);
        return true;
    }

    @Override
    public Boolean renameTableName(S3DataUnit dataUnit, String tablename) {
        throw new UnsupportedOperationException("S3DataUnitService can not support this method.");
    }
}
