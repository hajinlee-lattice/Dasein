package com.latticeengines.datacloud.core.util;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import com.latticeengines.domain.exposed.camille.Path;

public final class S3PathBuilder {

    public static final String SUCCESS_FILE = "_SUCCESS";
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd_HH-mm-ss_z";
    public static final String UTC = "UTC";
    public static final String VERSION_FILE = "_CURRENT_VERSION";

    private static final String MATCH_PREFIX = "match_";
    private static final String JSON_FILE_EXTENSION = ".json";
    private static final String OUTPUT_FILE_SUFFIX = "_output";
    private static final String AVRO_FILE_EXTENSION = ".avro";
    private static final String ERR_FILE_EXTENSION = ".err";
    private static final String COLUMN_SELECTIONS = "ColumnSelections";
    private static final String UNDERSCORE = "_";
    private static final String HYPHEN = "-";
    private static final String BLOCK = "block_";
    private static final String INPUT = "Input";
    private static final String SCHEMA = "Schema";
    private static final String AVRO_SCHEMA_FILE_EXTENSION = ".avsc";
    private static final String WORK_FLOWS = "WorkFlows";
    private static final String SNAPSHOT = "Snapshot";
    private static final String INGESTION = "Ingestion";
    private static final String SOURCES = "Sources";
    private static final String PATH_SEPARATOR = "/";
    private static final String PROP_DATA = "PropData";
    private static final String SERVICES = "Services";
    private static final String MATCHES_SEGMENT = "Matches";
    private static final String BLOCKS_SEGMENT = "Blocks";
    private static final String RAW_DATA_FLOW_TYPE = "Raw";
    private static final String LATEST_FILE = "_LATEST_TIMESTAMP";
    private static final String PODS_ROOT = PATH_SEPARATOR + "Pods";
    private static final String COLLECTORS = "Collectors";

    private static volatile String defaultPod = null;

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone(UTC));
    }

    public static Path podDir() {
        return new Path(PODS_ROOT).append(HdfsPodContext.getHdfsPodId());
    }

    private static Path defaultPodDir() {
        // lazy load default pod
        if (defaultPod == null) {
            synchronized (HdfsPathBuilder.class) {
                if (defaultPod == null) {
                    defaultPod = HdfsPodContext.getDefaultHdfsPodId();
                }
            }
        }
        return new Path(PODS_ROOT).append(defaultPod);
    }

    public static Path propDataDir() {
        return podDir();
    }

    public static Path constructCollectorWorkerDir(String vendor, String workerId) {
        return defaultPodDir().append(COLLECTORS).append(vendor).append(workerId);
    }

    private static String constructPartialPath(String name) {
        return name.endsWith(PATH_SEPARATOR) ? name.substring(0, name.lastIndexOf(PATH_SEPARATOR)) : name;
    }

    public static Path constructIngestionDir(String ingestionName) {
        String partialPath = constructPartialPath(ingestionName);
        return propDataDir().append(INGESTION).append(partialPath);
    }

    public static Path constructLegacyBODCMostRecentDir() {
        return propDataDir().append(INGESTION).append("Legacy_MostRecent_BODC");
    }
}
