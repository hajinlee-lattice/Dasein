package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.apps.cdl.service.DropFolderService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Service("dropFolderService")
public class DropFolderServiceImpl implements DropFolderService {
    private static final Logger log = LoggerFactory.getLogger(DropFolderServiceImpl.class);

    private static final String[] objectsName = {
            BusinessEntity.Account.name(),
            BusinessEntity.Contact.name(),
            BusinessEntity.Product.name(),
            BusinessEntity.Transaction.name()
    };

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private S3Service s3Service;

    @Override
    public void createTenantDefaultFolder(String customerSpace) {
        customerSpace = CustomerSpace.shortenCustomerSpace(customerSpace);

        String dropBoxBucket = dropBoxService.getDropBoxBucket();
        String dropBoxPrefix = dropBoxService.getDropBoxPrefix();

        s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, customerSpace, null, null));

        for (String objectName : objectsName) {
            s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, customerSpace, objectName, null));
        }
    }

    @Override
    public void createFolder(String customerSpace, String objectName, String path) {
        customerSpace = CustomerSpace.shortenCustomerSpace(customerSpace);

        String dropBoxBucket = dropBoxService.getDropBoxBucket();
        String dropBoxPrefix = dropBoxService.getDropBoxPrefix();

        s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, customerSpace, formatPath(objectName), null));
        String[] folderList = path.split("/");
        String needCreateFolder = "";
        for (String folder : folderList) {
            if (StringUtils.isNotEmpty(folder)) {
                needCreateFolder += "/" + folder;
                s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, customerSpace,
                        formatPath(objectName), formatPath(needCreateFolder)));
            }
        }
    }

    @Override
    public List<String> getDropFolders(String customerSpace, String objectName, String path) {
        customerSpace = CustomerSpace.shortenCustomerSpace(customerSpace);

        String dropBoxBucket = dropBoxService.getDropBoxBucket();
        String dropBoxPrefix = dropBoxService.getDropBoxPrefix();

        return s3Service.listSubFolders(dropBoxBucket,
                getFullPath(dropBoxPrefix, customerSpace, formatPath(objectName), formatPath(path)));
    }

    private String getFullPath(String dropBoxPrefix, String customerSpace, String objectName, String path) {
        String fullPath = dropBoxPrefix + "/" + customerSpace;

        if (StringUtils.isNotEmpty(objectName)) {
            fullPath += "/" + objectName;

            if (StringUtils.isNotEmpty(path)) {
                fullPath += "/" + path;
            }
        }

        return fullPath;
    }

    private String formatPath(String path) {
        if (StringUtils.isNotEmpty(path)) {
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            if (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
        }

        return path;
    }
}
