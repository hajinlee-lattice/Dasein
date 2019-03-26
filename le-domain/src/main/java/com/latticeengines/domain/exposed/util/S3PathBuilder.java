package com.latticeengines.domain.exposed.util;

import org.apache.commons.lang3.StringUtils;

public final class S3PathBuilder {


    private static String uiDisplayS3Dir = "%s/dropfolder/%s/%s/Templates/%s/";

    private static String uiDisplayS3Dir_old = "%s/dropfolder/%s/Templates/%s/";

    private static final String SPLIT_CHART = "_";

    public static String getUiDisplayS3Dir(String bucket, String dropBox, String folderName) {
        return S3PathBuilder.getUiDisplayS3Dir(bucket, dropBox, getSystemNameFromFeedType(folderName),
                getFolderNameFromFeedType(folderName));
    }

    public static String getUiDisplayS3Dir(String bucket, String dropBox, String systemName, String folderName) {
        if (!StringUtils.isEmpty(systemName))
            return String.format(uiDisplayS3Dir, bucket, dropBox, systemName, folderName);
        else
            return String.format(uiDisplayS3Dir_old, bucket, dropBox, folderName);
    }

    public static String getSystemNameFromFeedType(String feedType) {//new path feedType=systemName#folderName
        if (StringUtils.isEmpty(feedType)) {
            return StringUtils.EMPTY;
        }
        if (!feedType.contains(SPLIT_CHART))
            return StringUtils.EMPTY;
        String systemName = feedType.substring(0, feedType.lastIndexOf(SPLIT_CHART));
        return systemName;
    }

    public static String getFolderNameFromFeedType(String feedType) {
        if (StringUtils.isEmpty(feedType)) {
            return StringUtils.EMPTY;
        }
        if (!feedType.contains(SPLIT_CHART))
            return feedType;
        String folderName = feedType.substring(feedType.lastIndexOf(SPLIT_CHART)+1);
        if (StringUtils.isEmpty(folderName))
            throw new IllegalArgumentException(String.format("Cannot parse feedType %s", feedType));
        return folderName;
    }

}
