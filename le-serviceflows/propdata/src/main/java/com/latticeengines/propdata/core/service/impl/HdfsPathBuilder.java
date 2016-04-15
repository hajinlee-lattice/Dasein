package com.latticeengines.propdata.core.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.propdata.core.source.Source;

@Component("hdfsPathBuilder")
public class HdfsPathBuilder {

    private static final String MATCH_PREFIX = "match_";
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd_HH-mm-ss_z";
    private static final String UTC = "UTC";
    private static final String DEFAULT_POD_ID = "Default";
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
    private static final String INGESTION_FIREHOSE = "IngestionFirehose";
    private static final String SOURCES = "Sources";
    private static final String PATH_SEPARATOR = "/";
    private static final String PROP_DATA = "PropData";
    private static final String SERVICES = "Services";
    private static final String MATCHES_SEGMENT = "Matches";
    private static final String BLOCKS_SEGMENT = "Blocks";
    private static final String RAW_DATA_FLOW_TYPE = "Raw";
    private static final String VERSION_FILE = "_CURRENT_VERSION";
    private static final String LATEST_FILE = "_LATEST_TIMESTAMP";
    private static final String PODS_ROOT = PATH_SEPARATOR + "Pods";

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone(UTC));
    }

    private ThreadLocal<String> podId = new ThreadLocal<>();

    @PostConstruct
    private void postConstruct() {
        podId.set(CamilleEnvironment.getPodId());
    }

    public Path podDir() {
        return new Path(PODS_ROOT).append(podId.get());
    }

    public Path propDataDir() {
        return podDir().append(SERVICES).append(PROP_DATA);
    }

    public Path constructSourceDir(Source source) {
        String sourceName = source.getSourceName();
        sourceName = sourceName.endsWith(PATH_SEPARATOR)
                ? sourceName.substring(0, sourceName.lastIndexOf(PATH_SEPARATOR)) : sourceName;
        return propDataDir().append(SOURCES).append(sourceName);
    }

    public Path constructRawDir(Source source) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append(RAW_DATA_FLOW_TYPE);
    }

    public Path constructSnapshotRootDir(Source source) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append(SNAPSHOT);
    }

    public Path constructSnapshotDir(Source source, String version) {
        Path baseDir = constructSnapshotRootDir(source);
        return baseDir.append(version);
    }

    public Path constructWorkFlowDir(Source source, String flowName) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append(WORK_FLOWS).append(flowName);
    }

    public Path constructSchemaFile(Source source, String version) {
        Path baseDir = constructSourceDir(source);
        String avscFile = source.getSourceName() + AVRO_SCHEMA_FILE_EXTENSION;
        return baseDir.append(SCHEMA).append(version).append(avscFile);
    }

    public Path constructVersionFile(Source source) {
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

    public Path constructMatchErrorFile(String rootOperationUid) {
        String fileName = MATCH_PREFIX + replaceHyphenAndMakeLowercase(rootOperationUid) + ERR_FILE_EXTENSION;
        return constructMatchOutputDir(rootOperationUid).append(fileName);
    }

    public Path constructMatchSchemaFile(String rootOperationUid) {
        String fileName = MATCH_PREFIX + replaceHyphenAndMakeLowercase(rootOperationUid) + AVRO_SCHEMA_FILE_EXTENSION;
        return constructMatchOutputDir(rootOperationUid).append(fileName);
    }

    public Path constructMatchOutputFile(String rootOperationUid) {
        String fileName = MATCH_PREFIX + replaceHyphenAndMakeLowercase(rootOperationUid) + OUTPUT_FILE_SUFFIX
                + JSON_FILE_EXTENSION;
        return constructMatchOutputDir(rootOperationUid).append(fileName);
    }

    public Path constructMatchBlockDir(String rootOperationUid, String blockOperationUid) {
        return constructMatchDir(rootOperationUid).append(BLOCKS_SEGMENT).append(blockOperationUid);
    }

    public Path constructMatchBlockAvro(String rootOperationUid, String blockOperationUid) {
        String fileName = BLOCK + replaceHyphenAndMakeLowercase(blockOperationUid) + AVRO_FILE_EXTENSION;
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

    public Path predefinedColumnSelectionDir(ColumnSelection.Predefined predefined) {
        return propDataDir().append(COLUMN_SELECTIONS).append(predefined.getName());
    }

    public Path predefinedColumnSelectionFile(ColumnSelection.Predefined predefined, String version) {
        return predefinedColumnSelectionDir(predefined).append(predefined.getJsonFileName(version));
    }

    public Path predefinedColumnSelectionVersionFile(ColumnSelection.Predefined predefined) {
        return predefinedColumnSelectionDir(predefined).append(VERSION_FILE);
    }

    public Path constructIngestionDir(String ingestionName) {
        String partialPath = constructPartialPath(ingestionName);
        return propDataDir().append(INGESTION_FIREHOSE).append(partialPath);
    }

    public Path constructIngestionDir(String ingestionName, String version) {
        Path baseDir = constructIngestionDir(ingestionName);
        return baseDir.append(version);
    }

    public void changeHdfsPodId(String podId) {
        this.podId.set(podId);
    }

    private String replaceHyphenAndMakeLowercase(String text) {
        return text.replace(HYPHEN, UNDERSCORE).toLowerCase();
    }

    private String constructPartialPath(String name) {
        return name.endsWith(PATH_SEPARATOR) ? name.substring(0, name.lastIndexOf(PATH_SEPARATOR)) : name;
    }
}
