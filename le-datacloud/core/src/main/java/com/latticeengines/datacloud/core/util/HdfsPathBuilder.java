package com.latticeengines.datacloud.core.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.HasSqlPresence;
import com.latticeengines.datacloud.core.source.IngestedRawSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.TransformedToAvroSource;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

@Component("hdfsPathBuilder")
public class HdfsPathBuilder {

    public static final String SUCCESS_FILE = "_SUCCESS";
    public static final String DATE_FORMAT_STRING = "yyyy-MM-dd_HH-mm-ss_z";
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
    private static final String BLOCKS_ERROR_SEGMENT = "BlocksError";
    // newly allocated entities for entity match
    private static final String NEW_ENTITIES = "NewEntities";
    private static final String BLOCKS_NEW_ENTITY_SEGMENT = "BlockNewEntities";
    private static final String RAW_DATA_FLOW_TYPE = "Raw";
    private static final String LATEST_FILE = "_LATEST_TIMESTAMP";
    private static final String PODS_ROOT = PATH_SEPARATOR + "Pods";
    private static final String COLLECTORS = "Collectors";
    private static final String LEGACY_BODC_CONSOLIDATION = "MostRecent_Legacy";

    private static volatile String defaultPod = null;

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone(UTC));
    }

    public Path podDir() {
        return new Path(PODS_ROOT).append(HdfsPodContext.getHdfsPodId());
    }

    private Path defaultPodDir() {
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

    public Path propDataDir() {
        return podDir().append(SERVICES).append(PROP_DATA);
    }

    private Path defaultPropDataDir() {
        return defaultPodDir().append(SERVICES).append(PROP_DATA);
    }

    public Path constructTransformationSourceDir(Source source) {
        return constructTransformationSourceDir(source, null);
    }

    public Path constructTransformationSourceDir(Source source, String version) {
        Path dir = null;
        if (source instanceof TransformedToAvroSource || source instanceof IngestedRawSource) {
            dir = constructRawDir(source);
        } else if (source instanceof IngestionSource) {
            dir = constructIngestionDir(((IngestionSource) source).getIngestionName());
        } else if (source instanceof DerivedSource || source instanceof HasSqlPresence) {
            dir = constructSnapshotRootDir(source.getSourceName());
        } else {
            dir = constructSourceDir(source.getSourceName());
        }
        if (StringUtils.isNotBlank(version)) {
            dir = dir.append(version);
        }
        return dir;
    }

    public Path constructTablePath(String tableName, CustomerSpace customerSpace, String namespace) {
        return PathBuilder.buildDataTablePath(getHdfsPodId(), customerSpace, namespace).append(tableName);
    }

    public Path constructTableSchemaFilePath(String tableName, CustomerSpace customerSpace, String namespace) {
        String avscFile = tableName + AVRO_SCHEMA_FILE_EXTENSION;
        return PathBuilder.buildDataTableSchemaPath(getHdfsPodId(), customerSpace, namespace).append(tableName)
                .append(avscFile);
    }

    public Path constructSourceBaseDir() {
        return propDataDir().append(SOURCES);
    }

    @Deprecated
    public Path constructSourceDir(Source source) {
        String sourceName = source.getSourceName();
        return constructSourceDir(sourceName);
    }

    public Path constructSourceDir(String sourceName) {
        sourceName = sourceName.endsWith(PATH_SEPARATOR)
                ? sourceName.substring(0, sourceName.lastIndexOf(PATH_SEPARATOR)) : sourceName;
        return propDataDir().append(SOURCES).append(sourceName);
    }

    public Path constructRawDir(Source source) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append(RAW_DATA_FLOW_TYPE);
    }

    @Deprecated
    public Path constructSnapshotRootDir(Source source) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append(SNAPSHOT);
    }

    public Path constructSnapshotRootDir(String sourceName) {
        Path baseDir = constructSourceDir(sourceName);
        return baseDir.append(SNAPSHOT);
    }

    @Deprecated
    public Path constructSnapshotDir(Source source, String version) {
        Path baseDir = constructSnapshotRootDir(source);
        return baseDir.append(version);
    }

    public Path constructSnapshotDir(String sourceName, String version) {
        Path baseDir = constructSnapshotRootDir(sourceName);
        return baseDir.append(version);
    }

    public Path constructWorkFlowDir(Source source, String flowName) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append(WORK_FLOWS).append(flowName);
    }

    @Deprecated
    public Path constructSchemaFile(Source source, String version) {
        return constructSchemaFile(source.getSourceName(), version);
    }

    public Path constructSchemaFile(String sourceName, String version) {
        Path baseDir = constructSchemaDir(sourceName, version);
        String avscFile = sourceName + AVRO_SCHEMA_FILE_EXTENSION;
        return baseDir.append(avscFile);
    }

    public Path constructSchemaDir(String sourceName, String version) {
        Path baseDir = constructSourceDir(sourceName);
        return baseDir.append(SCHEMA).append(version);
    }

    public Path constructVersionFile(Ingestion ingestion) {
        Path baseDir = constructIngestionDir(ingestion.getIngestionName());
        return baseDir.append(VERSION_FILE);
    }

    public Path constructVersionFile(Source source) {
        Path baseDir = null;
        if (source instanceof IngestionSource) {
            baseDir = constructIngestionDir(((IngestionSource) source).getIngestionName());
        } else {
            baseDir = constructSourceDir(source.getSourceName());
        }
        return baseDir.append(VERSION_FILE);
    }

    public Path constructVersionFile(String source) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append(VERSION_FILE);
    }

    public Path constructLatestFile(Source source) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append(LATEST_FILE);
    }

    public Path constructRawIncrementalDir(Source source, Date archiveDate) {
        Path baseDir = constructRawDir(source);
        return baseDir.append(dateFormat.format(archiveDate));
    }

    private Path constructMatchDir(String rootOperationUid) {
        return propDataDir().append(MATCHES_SEGMENT).append(rootOperationUid);
    }

    public Path constructMatchInputDir(String rootOperationUid) {
        return constructMatchDir(rootOperationUid).append(INPUT);
    }

    public Path constructMatchBlockInputAvro(String rootOperationUid, String blockOperationUid) {
        String fileName = BLOCK + replaceHyphenAndMakeLowercase(blockOperationUid) + AVRO_FILE_EXTENSION;
        return constructMatchInputDir(rootOperationUid).append(fileName);
    }

    public Path constructMatchOutputDir(String rootOperationUid) {
        return constructMatchDir(rootOperationUid).append("Output");
    }

    public Path constructMatchErrorDir(String rootOperationUid) {
        return constructMatchDir(rootOperationUid).append("Error");
    }

    public Path constructMatchNewEntityDir(String rootOperationUid) {
        return constructMatchDir(rootOperationUid).append(NEW_ENTITIES);
    }

    public Path constructMatchBlockErrorSplitAvro(String rootOperationUid, String blockOperationUid, int split) {
        String fileName = getBlockFileNameWithSplit(blockOperationUid, split);
        return constructMatchBlockErrorDir(rootOperationUid, blockOperationUid).append(fileName);
    }

    public Path constructMatchBlockNewEntitySplitAvro(String rootOperationUid, String blockOperationUid, int split) {
        String fileName = getBlockFileNameWithSplit(blockOperationUid, split);
        return constructMatchBlockNewEntityDir(rootOperationUid, blockOperationUid).append(fileName);
    }

    public Path constructMatchErrorFile(String rootOperationUid) {
        String fileName = MATCH_PREFIX + replaceHyphenAndMakeLowercase(rootOperationUid) + ERR_FILE_EXTENSION;
        return constructMatchOutputDir(rootOperationUid).append(fileName);
    }

    public Path constructMatchSchemaFile(String rootOperationUid) {
        String fileName = MATCH_PREFIX + replaceHyphenAndMakeLowercase(rootOperationUid) + AVRO_SCHEMA_FILE_EXTENSION;
        return constructMatchOutputDir(rootOperationUid).append(fileName);
    }

    public Path constructMatchSchemaFileAtRoot(String rootOperationUid) {
        String fileName = MATCH_PREFIX + replaceHyphenAndMakeLowercase(rootOperationUid) + AVRO_SCHEMA_FILE_EXTENSION;
        return constructMatchDir(rootOperationUid).append(fileName);
    }

    public Path constructMatchOutputFile(String rootOperationUid) {
        String fileName = MATCH_PREFIX + replaceHyphenAndMakeLowercase(rootOperationUid) + OUTPUT_FILE_SUFFIX
                + JSON_FILE_EXTENSION;
        return constructMatchOutputDir(rootOperationUid).append(fileName);
    }

    public Path constructMatchBlockDir(String rootOperationUid, String blockOperationUid) {
        return constructMatchDir(rootOperationUid).append(BLOCKS_SEGMENT).append(blockOperationUid);
    }

    public Path constructMatchBlockErrorDir(String rootOperationUid, String blockOperationUid) {
        return constructMatchDir(rootOperationUid).append(BLOCKS_ERROR_SEGMENT).append(blockOperationUid);
    }

    public Path constructMatchBlockNewEntityDir(String rootOperationUid, String blockOperationUid) {
        return constructMatchDir(rootOperationUid).append(BLOCKS_NEW_ENTITY_SEGMENT).append(blockOperationUid);
    }

    public String constructMatchBlockAvroGlob(String rootOperationUid, String blockOperationUid) {
        return constructMatchBlockDir(rootOperationUid, blockOperationUid).toString() + "/*.avro";
    }

    public String constructMatchBlockErrorAvroGlob(String rootOperationUid, String blockOperationUid) {
        return constructMatchBlockErrorDir(rootOperationUid, blockOperationUid).toString() + "/*.avro";
    }

    public String constructMatchBlockNewEntityAvroGlob(String rootOperationUid, String blockOperationUid) {
        return constructMatchBlockNewEntityDir(rootOperationUid, blockOperationUid).toString() + "/*.avro";
    }

    public Path constructMatchBlockSplitAvro(String rootOperationUid, String blockOperationUid, int split) {
        String fileName = getBlockFileNameWithSplit(blockOperationUid, split);
        return constructMatchBlockDir(rootOperationUid, blockOperationUid).append(fileName);
    }

    public Path constructMatchBlockErrorFile(String rootOperationUid, String blockOperationUid) {
        String fileName = BLOCK + replaceHyphenAndMakeLowercase(blockOperationUid) + ERR_FILE_EXTENSION;
        return constructMatchBlockDir(rootOperationUid, blockOperationUid).append(fileName);
    }

    public Path constructMatchBlockOutputFile(String rootOperationUid, String blockOperationUid) {
        String fileName = BLOCK + replaceHyphenAndMakeLowercase(blockOperationUid) + OUTPUT_FILE_SUFFIX
                + JSON_FILE_EXTENSION;
        return constructMatchBlockDir(rootOperationUid, blockOperationUid).append(fileName);
    }

    public Path predefinedColumnSelectionDir(Predefined predefined) {
        return propDataDir().append(COLUMN_SELECTIONS).append(predefined.getName());
    }

    public Path predefinedColumnSelectionFile(Predefined predefined, String version) {
        return predefinedColumnSelectionDir(predefined).append(predefined.getJsonFileName(version));
    }

    public Path predefinedColumnSelectionVersionFile(Predefined predefined) {
        return predefinedColumnSelectionDir(predefined).append(VERSION_FILE);
    }

    public Path constructIngestionDir(String ingestionName) {
        String partialPath = constructPartialPath(ingestionName);
        return propDataDir().append(INGESTION).append(partialPath);
    }

    public Path constructIngestionDir(String ingestionName, String version) {
        Path baseDir = constructIngestionDir(ingestionName);
        return baseDir.append(version);
    }

    public void changeHdfsPodId(String podId) {
        HdfsPodContext.changeHdfsPodId(podId);
    }

    public Path constructCollectorWorkerDir(String vendor, String workerId) {
        return defaultPropDataDir().append(COLLECTORS).append(vendor).append(workerId);
    }

    public Path constructBODCLegacyConsolidatonResultDir(String vendor) {
        return propDataDir().append(SOURCES).append(LEGACY_BODC_CONSOLIDATION).append(vendor);
    }

    @VisibleForTesting
    String getHdfsPodId() {
        return HdfsPodContext.getHdfsPodId();
    }

    private String replaceHyphenAndMakeLowercase(String text) {
        return text.replace(HYPHEN, UNDERSCORE).toLowerCase();
    }

    private String constructPartialPath(String name) {
        return name.endsWith(PATH_SEPARATOR) ? name.substring(0, name.lastIndexOf(PATH_SEPARATOR)) : name;
    }

    private String getBlockFileNameWithSplit(String blockOperationUid, int split) {
        return BLOCK + AvroUtils.getAvroFriendlyString(blockOperationUid) + String.format("-p-%05d", split)
                + AVRO_FILE_EXTENSION;
    }
}
