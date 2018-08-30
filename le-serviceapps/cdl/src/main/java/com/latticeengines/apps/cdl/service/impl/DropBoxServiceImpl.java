package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.DropBoxEntityMgr;
import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.cdl.DropBox;

@Service("dropBoxService")
public class DropBoxServiceImpl implements DropBoxService {

    private static final String DROPBOX = "dropbox";

    @Inject
    private DropBoxEntityMgr entityMgr;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String customersBucket;

    @Override
    public DropBox create() {
        DropBox dropbox = entityMgr.createDropBox();
        String prefix = toPrefix(dropbox);
        if (!s3Service.isNonEmptyDirectory(customersBucket, prefix)) {
            s3Service.createFolder(customersBucket, prefix);
        }
        return dropbox;
    }

    @Override
    public void delete() {
        DropBox dropbox = entityMgr.getDropBox();
        if (dropbox != null) {
            String prefix = toPrefix(dropbox);
            s3Service.cleanupPrefix(customersBucket, prefix);
        }
        entityMgr.delete(dropbox);
    }

    @Override
    public String getDropBoxBucket() {
        return customersBucket;
    }

    @Override
    public String getDropBoxPrefix() {
        DropBox dropbox = entityMgr.getDropBox();
        if (dropbox == null) {
            return null;
        } else {
            return toPrefix(dropbox);
        }
    }

    private String toPrefix(DropBox dropbox) {
        return DROPBOX + "/" + dropbox.getDropBox();
    }

}
