package com.latticeengines.propdata.matching.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import javax.annotation.Resource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.latticeengines.cassandra.exposed.dao.CassandraGenericDao;
import com.latticeengines.cassandra.exposed.dao.data.Column;
import com.latticeengines.cassandra.exposed.dao.data.ColumnType;
import com.latticeengines.cassandra.exposed.dao.data.ColumnTypeMapper;
import com.latticeengines.cassandra.exposed.dao.data.Table;
import com.latticeengines.propdata.matching.service.MatchService;
import com.latticeengines.propdata.matching.service.SourceSpec;

@Component("matchService")
public class MatchServicempl implements MatchService {

    @Resource(name = "propDataCassandraGenericDao")
    private CassandraGenericDao dao;

    @Resource(name = "poolService")
    private ExecutorService pool;

    @Override
    public Map<String, Map<String, Object>> match(String domain, List<String> sources) {

        if (StringUtils.isEmpty(domain) || CollectionUtils.isEmpty(sources)) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, Object>> result = new HashMap<String, Map<String, Object>>();
        for (String sourceName : sources) {
            SourceSpec sourceSpec = SourceEnum.getConnectorSpec(sourceName);
            if (sourceSpec == null) {
                System.out.println("Source is not supported!.");
                continue;
            }

            String cql = "SELECT * FROM " + sourceSpec.getSourceName();
            ResultSet resultSet = dao.query(cql);
            Row row = null;
            if ((row = resultSet.one()) != null) {
                Map<String, Object> map = ColumnTypeMapper.convertRowToMap(row);
                result.put(sourceName, map);
            }
        }

        return result;
    }

    @Override
    public void createDomainIndex(String sourceName) {
        SourceSpec sourceSpec = SourceEnum.getConnectorSpec(sourceName);
        if (sourceSpec == null) {
            System.out.println("Source is not supported!.");
            return;
        }

        String sourceDomainColumn = sourceSpec.getSourceDomainColumn();
        String sourceKeyColumn = sourceSpec.getSourceKey();
        String indexKeyColumn = sourceSpec.getIndexKeyColumn();

        String domainIndexTable = "Domain_Index";

        Table table = Table.newInstance(domainIndexTable);
        table.addPartitionColumn(new Column("domain_name", ColumnType.VARCHAR));
        dao.createTable(table);

        String limit = 999_999_999 + "";
        indexTable(domainIndexTable, sourceName, sourceDomainColumn, sourceKeyColumn, indexKeyColumn, limit);
    }

    private void indexTable(String domainIndexTable, String sourceTableName, String sourceDomainColumn,
            String sourceKeyColumn, String indexKeyColumn, String limit) {

        dao.dropColumn(domainIndexTable, sourceTableName);
        dao.addColumn(domainIndexTable, new Column(sourceTableName, ColumnType.VARCHAR));
        String cql = "SELECT " + sourceKeyColumn + ", \"" + sourceDomainColumn + "\" FROM \"" + sourceTableName
                + "\" LIMIT " + limit;

        ResultSet resultSet = dao.query(cql);
        Row row = null;

        int countWithDomain = 0;
        int countWithoutDomain = 0;
        long startTime = System.currentTimeMillis();
        while ((row = resultSet.one()) != null) {
            Map<String, Object> map = ColumnTypeMapper.convertRowToMap(row);
            Map<String, Object> indexMap = getIndexMapFromSourceMap(map, sourceDomainColumn, sourceKeyColumn,
                    indexKeyColumn, sourceTableName);
            if (indexMap == null) {
                countWithoutDomain++;
                continue;
            }
            countWithDomain++;

            submitTask(domainIndexTable, indexMap);
        }

        System.out.println("Table=" + sourceTableName + ", total row count for rows with domain is=" + countWithDomain);
        System.out.println("Table=" + sourceTableName + ", total row count for rows WITHOUT domain is="
                + countWithoutDomain);
        System.out.println("Table=" + sourceTableName + ", total time=" + (System.currentTimeMillis() - startTime)
                / 1000 + " seconds.");
    }

    private void submitTask(String domainIndexTable, Map<String, Object> indexMap) {

        // dao.save(table, map);

        IndexTask<Boolean> task = new IndexTask<>(domainIndexTable, indexMap);
        pool.submit(task);
    }

    private Map<String, Object> getIndexMapFromSourceMap(Map<String, Object> map, String sourceDomainColumn,
            String sourceKeyColumn, String indexKeyName, String indexColumn) {
        Object domain = map.get(sourceDomainColumn);
        String uuid = null;
        if (domain == null || domain == "") {
            return null;
        }
        uuid = (String) map.get(sourceKeyColumn);
        String domainStr = domain.toString().toLowerCase();
        Map<String, Object> indexMap = new HashMap<>();
        indexMap.put(indexKeyName, domainStr);
        indexMap.put(indexColumn, uuid);
        return indexMap;
    }

    private class IndexTask<T> implements Callable<Boolean> {

        private String table;
        private Map<String, Object> map;

        public IndexTask(String table, Map<String, Object> map) {
            super();
            this.table = table;
            this.map = map;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                dao.save(table, map);
                return Boolean.TRUE;

            } catch (Exception ex) {
                ex.printStackTrace();
                return Boolean.FALSE;
            }
        }

    }
}
