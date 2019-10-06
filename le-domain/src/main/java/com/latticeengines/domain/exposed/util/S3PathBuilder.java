package com.latticeengines.domain.exposed.util;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.query.EntityType;

public final class S3PathBuilder {

    private static String uiDisplayS3Dir_old = "%s/dropfolder/%s/Templates/%s/";

    private static final String SPLIT_CHART = "_";

    public static String getUiDisplayS3Dir(String bucket, String dropBox, String folderName) {
        return String.format(uiDisplayS3Dir_old, bucket, dropBox, folderName);
    }

    public static String getSystemNameFromFeedType(String feedType) {//new path feedType=systemName#folderName
        if (StringUtils.isEmpty(feedType)) {
            return StringUtils.EMPTY;
        }
        List<String> defaultFolders = EntityType.getDefaultFolders();
        for (String folderName : defaultFolders) {
            String suffix = SPLIT_CHART + folderName;
            if (feedType.endsWith(suffix)) {
                return feedType.substring(0, feedType.lastIndexOf(suffix));
            }
        }
        return StringUtils.EMPTY;
    }

    public static String getFolderNameFromFeedType(String feedType) {
        if (StringUtils.isEmpty(feedType)) {
            return StringUtils.EMPTY;
        }
        List<String> defaultFolders = EntityType.getDefaultFolders();
        for (String folderName : defaultFolders) {
            String suffix = SPLIT_CHART + folderName;
            if (feedType.endsWith(suffix)) {
                return folderName;
            }
        }
        return feedType;
    }

    public static String getFolderName(String systemName, String entityObjectName) {
        if (StringUtils.isEmpty(systemName)) {
            return entityObjectName;
        }
        return systemName + SPLIT_CHART + entityObjectName;
    }

    public static void setS3Bucket(S3DataUnit s3DataUnit) {
        String s3Url = s3DataUnit.getLinkedDir();
        Matcher matcher = Pattern.compile("^(s3a|s3n|s3)://(?<bucket>[^/]+)/(?<prefix>.*)").matcher(s3Url);
        if (matcher.matches()) {
            String bucket = matcher.group("bucket");
            String prefix = matcher.group("prefix");
            s3DataUnit.setBucketName(bucket);
            // if prefix end with a file, then extract path
            if (Pattern.compile(".*/.*\\..*$").matcher(prefix).matches()) {
                prefix = prefix.substring(0, prefix.lastIndexOf("/"));
            }
            s3DataUnit.setPrefix(prefix);
        }
    }

    public static void main(String[] args) {
        String linkedDir = "s3a://" + "test" + "/" + "test1/a.txt";
        S3DataUnit s3DataUnit = new S3DataUnit();
        s3DataUnit.setLinkedDir(linkedDir);
        setS3Bucket(s3DataUnit);
        System.out.println(s3DataUnit.getBucketName());
        System.out.println(s3DataUnit.getPrefix());
    }
}
