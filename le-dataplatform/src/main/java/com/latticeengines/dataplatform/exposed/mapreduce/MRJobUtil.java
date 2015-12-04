package com.latticeengines.dataplatform.exposed.mapreduce;

import java.net.URI;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class MRJobUtil {

    private static final String comma = ",";

    private static final Joiner commaJoiner = Joiner.on(", ").skipNulls();

    public static String getPlatformShadedJarPath(Configuration yarnConfiguration) {
         List<String> jarFilePaths = getPlatformShadedJarPathList(yarnConfiguration);
         return commaJoiner.join(jarFilePaths);
    }
    
    public static List<String> getPlatformShadedJarPathList(Configuration yarnConfiguration) {
        try {
            return HdfsUtils.getFilesForDir(yarnConfiguration, "/app/dataplatform/lib", ".*.jar$");
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002);
        }
    }

    public static void setLocalizedResources(Job mrJob, Properties properties) {
        String cachPaths = properties.getProperty(MapReduceProperty.CACHE_FILE_PATH.name());
        String cacheArchivePaths = properties.getProperty(MapReduceProperty.CACHE_ARCHIVE_PATH.name());
        MRJobUtil.setCacheFiles(mrJob, cachPaths);
        MRJobUtil.setCacheArchiveFiles(mrJob, cacheArchivePaths);
    }

    public static URI[] getCommaSeparatedURI(String paths) throws Exception {
        String[] files = paths.split(comma);
        URI[] filesURI = new URI[files.length];
        for (int i = 0; i < filesURI.length; i++) {
            filesURI[i] = new URI(files[i].trim());
        }
        return filesURI;
    }

    private static void setCacheFiles(Job mrJob, String cachePaths) {
        if (cachePaths == null)
            return;
        try {
            URI[] caches = getCommaSeparatedURI(cachePaths);
            mrJob.setCacheFiles(caches);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15009, e);
        }
    }

    private static void setCacheArchiveFiles(Job mrJob, String cacheArchivePaths) {
        if (cacheArchivePaths == null)
            return;
        try {
            URI[] cacheArchives = getCommaSeparatedURI(cacheArchivePaths);
            mrJob.setCacheArchives(cacheArchives);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15010, e);
        }
    }
}
