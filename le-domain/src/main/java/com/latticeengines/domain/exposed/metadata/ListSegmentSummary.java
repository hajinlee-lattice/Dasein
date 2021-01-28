package com.latticeengines.domain.exposed.metadata;

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
public class ListSegmentSummary implements Serializable {

    private static final long serialVersionUID = -8532367815438372760L;

    @JsonProperty("segment_name")
    private String segmentName;
    @JsonProperty("csv_adaptor")
    private CSVAdaptor csvAdaptor;
    @JsonProperty("s3_upload_dropfolder")
    private String s3UploadDropFolder;
    @JsonProperty("table_location")
    private String tableLocation;
    @JsonProperty("table_hdfs_location")
    private String tableHdfsLocation;
    @JsonProperty("table_name")
    private String tableName;
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

    public String getTableLocation() {
        return tableLocation;
    }

    public void setTableLocation(String tableLocation) {
        this.tableLocation = tableLocation;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getAthenaTableName() {
        return athenaTableName;
    }

    public void setAthenaTableName(String athenaTableName) {
        this.athenaTableName = athenaTableName;
    }

    public String getTableHdfsLocation() {
        return tableHdfsLocation;
    }

    public void setTableHdfsLocation(String tableHdfsLocation) {
        this.tableHdfsLocation = tableHdfsLocation;
    }
}
