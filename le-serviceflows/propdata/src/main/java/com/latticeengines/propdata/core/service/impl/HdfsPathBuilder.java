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

    private static final String MATCHES_SEGMENT = "Matches";
    private static final String BLOCKS_SEGMENT = "Blocks";

    private static final String rawDataFlowType = "Raw";
    private static final String versionFile = "_CURRENT_VERSION";
    private static final String latestFile = "_LATEST_TIMESTAMP";
    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_z");

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private String podId = "Default";

    @PostConstruct
    private void postConstruct() {
        podId = CamilleEnvironment.getPodId();
    }

    public Path podDir() {
        return new Path("/Pods").append(podId);
    }

    public Path propDataDir() {
        return podDir().append("Services").append("PropData");
    }

    public Path constructSourceDir(Source source) {
        String sourceName = source.getSourceName();
        sourceName = sourceName.endsWith("/") ? sourceName.substring(0, sourceName.lastIndexOf("/")) : sourceName;
        return propDataDir().append("Sources").append(sourceName);
    }

    public Path constructRawDir(Source source) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append(rawDataFlowType);
    }

    public Path constructSnapshotRootDir(Source source) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append("Snapshot");
    }

    public Path constructSnapshotDir(Source source, String version) {
        Path baseDir = constructSnapshotRootDir(source);
        return baseDir.append(version);
    }

    public Path constructWorkFlowDir(Source source, String flowName) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append("WorkFlows").append(flowName);
    }

    public Path constructSchemaFile(Source source, String version) {
        Path baseDir = constructSourceDir(source);
        String avscFile = source.getSourceName() + ".avsc";
        return baseDir.append("Schema").append(version).append(avscFile);
    }

    public Path constructVersionFile(Source source) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append(versionFile);
    }

    public Path constructLatestFile(Source source) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append(latestFile);
    }

    public Path constructRawIncrementalDir(Source source, Date archiveDate) {
        Path baseDir = constructRawDir(source);
        return baseDir.append(dateFormat.format(archiveDate));
    }

    private Path constructMatchDir(String rootOperationUid) {
        return propDataDir().append(MATCHES_SEGMENT).append(rootOperationUid);
    }

    public Path constructMatchInputDir(String rootOperationUid) {
        return constructMatchDir(rootOperationUid).append("Input");
    }

    public Path constructMatchOutputDir(String rootOperationUid) {
        return constructMatchDir(rootOperationUid).append("Output");
    }

    public Path constructMatchErrorFile(String rootOperationUid) {
        String fileName = "match_" + rootOperationUid.replace("-", "_").toLowerCase() + ".err";
        return constructMatchOutputDir(rootOperationUid).append(fileName);
    }

    public Path constructMatchSchemaFile(String rootOperationUid) {
        String fileName = "match_" + rootOperationUid.replace("-", "_").toLowerCase() + ".avsc";
        return constructMatchOutputDir(rootOperationUid).append(fileName);
    }

    public Path constructMatchOutputFile(String rootOperationUid) {
        String fileName = "match_" + rootOperationUid.replace("-", "_").toLowerCase() + "_output.json";
        return constructMatchOutputDir(rootOperationUid).append(fileName);
    }

    public Path constructMatchBlockDir(String rootOperationUid, String blockOperationUid) {
        return constructMatchDir(rootOperationUid).append(BLOCKS_SEGMENT).append(blockOperationUid);
    }

    public Path constructMatchBlockAvro(String rootOperationUid, String blockOperationUid) {
        String fileName = "block_" + blockOperationUid.replace("-", "_").toLowerCase() + ".avro";
        return constructMatchBlockDir(rootOperationUid, blockOperationUid).append(fileName);
    }

    public Path constructMatchBlockErrorFile(String rootOperationUid, String blockOperationUid) {
        String fileName = "block_" + blockOperationUid.replace("-", "_").toLowerCase() + ".err";
        return constructMatchBlockDir(rootOperationUid, blockOperationUid).append(fileName);
    }

    public Path constructMatchBlockOutputFile(String rootOperationUid, String blockOperationUid) {
        String fileName = "block_" + blockOperationUid.replace("-", "_").toLowerCase() + "_output.json";
        return constructMatchBlockDir(rootOperationUid, blockOperationUid).append(fileName);
    }

    public Path predefinedColumnSelectionDir(ColumnSelection.Predefined predefined) {
        return propDataDir().append("ColumnSelections").append(predefined.getName());
    }

    public Path predefinedColumnSelectionFile(ColumnSelection.Predefined predefined, String version) {
        return predefinedColumnSelectionDir(predefined).append(predefined.getJsonFileName(version));
    }

    public Path predefinedColumnSelectionVersionFile(ColumnSelection.Predefined predefined) {
        return predefinedColumnSelectionDir(predefined).append(versionFile);
    }

    public void changeHdfsPodId(String podId) {
        this.podId = podId;
    }
}
