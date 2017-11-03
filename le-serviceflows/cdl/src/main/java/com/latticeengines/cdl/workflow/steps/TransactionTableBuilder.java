package com.latticeengines.cdl.workflow.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component("transactionTableBuilder")
public class TransactionTableBuilder {

    private static final String TRANSACTION_DATE = InterfaceName.TransactionDate.name();
    private static String regex = "^" + BusinessEntity.Transaction.name()
            + "_(?:[0-9]{2})?[0-9]{2}-[0-3]?[0-9]-[0-3]?[0-9]$";
    protected static Pattern pattern = Pattern.compile(regex);

    @Autowired
    YarnConfiguration yarnConfiguration;

    public Table setupMasterTable(String tablePrefix, String pipelineVersion, Table aggregateTable) {
        try {
            List<String> dateFiles = getMasterDateFiles(aggregateTable);
            Table masterTable = createTable(dateFiles, tablePrefix, pipelineVersion);
            return masterTable;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public Table setupDeltaTable(String tablePrefix, String pipelineVersion, Table aggregateTable) {
        try {
            Set<String> dates = getDeltaDates(aggregateTable);
            List<String> dateFiles = getDeltaDateFiles(aggregateTable, dates);
            Table deltaTable = createTable(dateFiles, tablePrefix, pipelineVersion);
            return deltaTable;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private Set<String> getDeltaDates(Table aggregateTable) {
        String hdfsPath = aggregateTable.getExtracts().get(0).getPath();
        Set<String> dates = new HashSet<>();
        Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, hdfsPath);
        while (iter.hasNext()) {
            GenericRecord record = iter.next();
            String date = record.get(TRANSACTION_DATE).toString();
            if (!dates.contains(date))
                dates.add(date);
        }
        return dates;
    }

    protected List<String> getDeltaDateFiles(Table aggregateTable, Set<String> dates) {
        String baseDir = getTableBaseDir(aggregateTable);
        List<String> files = new ArrayList<>();
        for (String date : dates) {
            files.add(baseDir + "/" + TableUtils.getFullTableName(BusinessEntity.Transaction.name(), date));
        }
        return files;
    }

    private List<String> getMasterDateFiles(Table aggregateTable) throws IOException {
        String baseDir = getTableBaseDir(aggregateTable);
        HdfsFilenameFilter filter = getFilter();
        List<String> dateFiles = HdfsUtils.getFilesForDir(yarnConfiguration, baseDir, filter);
        return dateFiles;
    }

    protected HdfsFilenameFilter getFilter() {
        HdfsFilenameFilter filter = new HdfsFilenameFilter() {
            @Override
            public boolean accept(String fileName) {
                Matcher matcher = pattern.matcher(fileName);
                return matcher.matches();
            }
        };
        return filter;
    }

    protected String getTableBaseDir(Table aggregateTable) {
        String hdfsPath = aggregateTable.getExtracts().get(0).getPath();
        int index = 0;
        if (hdfsPath.endsWith("*.avro") || hdfsPath.endsWith("/")) {
            index = StringUtils.lastOrdinalIndexOf(hdfsPath, "/", 2);
        } else {
            index = StringUtils.lastOrdinalIndexOf(hdfsPath, "/", 1);
        }
        String hdfsDir = hdfsPath.substring(0, index);
        return hdfsDir;
    }

    protected Table createTable(List<String> dateFiles, String tableName, String pipelineVersion) {
        Table table = new Table();
        String fullTableName = TableUtils.getFullTableName(tableName, pipelineVersion);
        table.setName(fullTableName);
        table.setDisplayName(fullTableName);
        for (String file : dateFiles) {
            addExtract(table, file);
        }
        return table;
    }

    private void addExtract(Table masterTable, String file) {
        Extract extract = new Extract();
        extract.setName("extract");
        extract.setPath(file + "/*.avro");
        extract.setExtractionTimestamp(DateTime.now().getMillis());
        masterTable.addExtract(extract);
    }

}
