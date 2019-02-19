package com.latticeengines.domain.exposed.util;

public final class S3PathBuilder {
    private static String uiDisplayS3Dir = "%s/dropfolder/%s/Templates/%s/";

    public static String getUiDisplayS3Dir(String bucket, String dropBox, String folderName) {
        return String.format(uiDisplayS3Dir, bucket, dropBox, folderName);
    }

}
