package com.latticeengines.cdl.workflow.steps.callable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

public class QueryToTableIngestCallable extends RedshiftIngestCallable<String> {

    private final FrontEndQuery query;
    private final String tableName;
    private final EntityProxy entityProxy;
    private final MetadataProxy metadataProxy;
    private final DataCollection.Version version;
    private final String customerSpace;
    private final Schema schema;

    public static class Builder {
        private Configuration yarnConfiguration;
        private FrontEndQuery query;
        private String tableName;
        private EntityProxy entityProxy;
        private MetadataProxy metadataProxy;
        private DataCollection.Version version;
        private String customerSpace;
        private List<Pair<String, Class<?>>> schema;

        public Builder yarnConfiguration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
            return this;
        }

        public Builder query(FrontEndQuery query) {
            this.query = query;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder entityProxy(EntityProxy entityProxy) {
            this.entityProxy = entityProxy;
            return this;
        }

        public Builder metadataProxy(MetadataProxy metadataProxy) {
            this.metadataProxy = metadataProxy;
            return this;
        }

        public Builder version(DataCollection.Version version) {
            this.version = version;
            return this;
        }

        public Builder customerSpace(String customerSpace) {
            this.customerSpace = customerSpace;
            return this;
        }

        public Builder schema(List<Pair<String, Class<?>>> schema) {
            this.schema = schema;
            return this;
        }

        public QueryToTableIngestCallable build() {
            return new QueryToTableIngestCallable(yarnConfiguration, customerSpace, tableName, entityProxy,
                    metadataProxy, query, version, schema);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private QueryToTableIngestCallable(Configuration yarnConfiguration, String customerSpace, String tableName,
            EntityProxy entityProxy, MetadataProxy metadataProxy, FrontEndQuery query, DataCollection.Version version,
            List<Pair<String, Class<?>>> schema) {
        super(yarnConfiguration, getDataTablePath(customerSpace, tableName));
        if (query.getSort() == null) {
            throw new IllegalArgumentException("Must specify sort in the query, because we will use pagination.");
        }
        this.query = query;
        this.tableName = tableName;
        this.metadataProxy = metadataProxy;
        this.entityProxy = entityProxy;
        this.version = version;
        this.customerSpace = customerSpace;
        this.schema = AvroUtils.constructSchema(tableName, schema);
    }

    private static String getDataTablePath(String customerSpace, String tableName) {
        return PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(customerSpace), "")
                .append(tableName).toString();
    }

    @Override
    protected long getTotalCount() {
        FrontEndQuery query = JsonUtils.deserialize(JsonUtils.serialize(this.query), FrontEndQuery.class);
        return this.entityProxy.getCountFromObjectApi(customerSpace, query, version);
    }

    @Override
    protected DataPage fetchPage(long ingestedCount, long pageSize) {
        this.query.setPageFilter(new PageFilter(ingestedCount, pageSize));
        return this.entityProxy.getDataFromObjectApi(customerSpace, query, version);
    }

    @Override
    protected GenericRecord parseData(Map<String, Object> data) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Map.Entry<String, Object> elem : data.entrySet()) {
            builder.set(elem.getKey(), elem.getValue());
        }
        return builder.build();
    }

    @Override
    protected Schema getAvroSchema() {
        return this.schema;
    }

    @Override
    protected void preIngest() {
        String tableDataPath = getHdfsPath();
        try {
            if (HdfsUtils.fileExists(getYarnConfiguration(), tableDataPath)) {
                throw new IllegalArgumentException("Target table path " + tableDataPath + " is already occupied.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to check target table path.", e);
        }
    }

    @Override
    protected String postIngest() {
        if (getIngestedCount() > 0) {
            Table resultTable = MetadataConverter.getTable(getYarnConfiguration(), getHdfsPath(), null, null, true);
            resultTable.getExtracts().get(0).setProcessedRecords(getIngestedCount());
            resultTable.setName(tableName);
            metadataProxy.createTable(customerSpace, tableName, resultTable);
            return tableName;
        } else {
            // empty table, returns empty string
            return "";
        }
    }

}
