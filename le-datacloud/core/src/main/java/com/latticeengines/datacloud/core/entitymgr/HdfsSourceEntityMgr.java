package com.latticeengines.datacloud.core.entitymgr;

import java.util.Date;
import java.util.List;

import com.latticeengines.datacloud.core.source.IngestedRawSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import org.apache.avro.Schema;

public interface HdfsSourceEntityMgr {

    String getCurrentVersion(Source source);

    String getCurrentVersion(String sourceName);

    void setCurrentVersion(Source source, String version);

    void setCurrentVersion(String source, String version);

    void setLatestTimestamp(IngestedRawSource source, Date timestamp);

    Date getLatestTimestamp(IngestedRawSource source);

    Table getTableAtVersion(Source source, String version);

    Table getTableAtVersions(Source source, List<String> versions);

    Schema getAvscSchemaAtVersion(String sourceName, String version);

    /**
     * This is to fill in more detail about the table, after generating avros
     * Source service has another method to create a shell table before avro generation
     */
    TableSource materializeTableSource(String tableName, CustomerSpace customerSpace);

    Table getCollectedTableSince(IngestedRawSource source, Date earliest);

    Long count(Source source, String version);

    void purgeSourceAtVersion(Source source, String version);

    List<String> getVersions(Source source);

    void initiateSource(Source source);

    void deleteSource(Source source);

    boolean checkSourceExist(Source source);

    boolean checkSourceExist(Source source, String version);

    String getRequest(Source source, String requestName);

    boolean saveReport(Source source, String reportName, String version, String report);
}
