package com.latticeengines.domain.exposed.cdl.dashboard;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class TargetAccountList implements Serializable {

    private static final long serialVersionUID = -8532367815438155760L;

    @JsonProperty("name")
    private String segmentName;

    @JsonProperty("mappings")
    private CSVAdaptor csvAdaptor;

    @JsonProperty("s3_drop_folder")
    private String s3UploadDropFolder;

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("s3_path")
    private String s3Path;

    @JsonProperty("hdfs_path")
    private String hdfsPath;

    @JsonProperty("athena_table_name")
    private String athenaTableName;

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }

    public CSVAdaptor getCsvAdaptor() {
        return csvAdaptor;
    }

    public void setCsvAdaptor(CSVAdaptor csvAdaptor) {
        this.csvAdaptor = csvAdaptor;
    }

    public String getS3UploadDropFolder() {
        return s3UploadDropFolder;
    }

    public void setS3UploadDropFolder(String s3UploadDropFolder) {
        this.s3UploadDropFolder = s3UploadDropFolder;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getS3Path() {
        return s3Path;
    }

    public void setS3Path(String s3Path) {
        this.s3Path = s3Path;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }

    public String getAthenaTableName() {
        return athenaTableName;
    }

    public void setAthenaTableName(String athenaTableName) {
        this.athenaTableName = athenaTableName;
    }
}
