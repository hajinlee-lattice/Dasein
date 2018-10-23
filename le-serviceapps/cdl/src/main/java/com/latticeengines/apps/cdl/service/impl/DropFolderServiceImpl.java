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

@Service("dropFolderService")
public class DropFolderServiceImpl implements DropFolderService {
    private static final Logger log = LoggerFactory.getLogger(DropFolderServiceImpl.class);

    private static final String PS_SHARE = "PS_SHARE";

    private static final String TEMPLATES = "Templates";

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private S3Service s3Service;

    @Override
    public void createTenantDefaultFolder(String customerSpace) {
        String dropBoxBucket = dropBoxService.getDropBoxBucket();
        String dropBoxPrefix = dropBoxService.getDropBoxPrefix();

        s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, TEMPLATES, null, null));

        s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, TEMPLATES, PS_SHARE, null));
    }

    @Override
    public void createFolder(String customerSpace, String objectName, String path) {
        String dropBoxBucket = dropBoxService.getDropBoxBucket();
        String dropBoxPrefix = dropBoxService.getDropBoxPrefix();

        s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, TEMPLATES, formatPath(objectName), null));
        if (StringUtils.isNotEmpty(path)) {
            String[] folderList = path.split("/");
            String needCreateFolder = "";
            for (String folder : folderList) {
                if (StringUtils.isNotEmpty(folder)) {
                    needCreateFolder += "/" + folder;
                    s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, TEMPLATES,
                            formatPath(objectName), formatPath(needCreateFolder)));
                }
            }
        }
    }

    @Override
    public List<String> getDropFolders(String customerSpace, String objectName, String path) {
        String dropBoxBucket = dropBoxService.getDropBoxBucket();
        String dropBoxPrefix = dropBoxService.getDropBoxPrefix();

        return s3Service.listSubFolders(dropBoxBucket,
                getFullPath(dropBoxPrefix, TEMPLATES, formatPath(objectName), formatPath(path)));
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
