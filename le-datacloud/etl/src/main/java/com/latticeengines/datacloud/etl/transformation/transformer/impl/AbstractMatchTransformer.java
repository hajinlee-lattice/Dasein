package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

abstract class AbstractMatchTransformer extends AbstractTransformer<MatchTransformerConfig> {

    private static final Logger log = LoggerFactory.getLogger(AbstractTransformer.class);

    @Inject
    protected HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private Configuration yarnConfiguration;

    @Override
    public boolean validateConfig(MatchTransformerConfig config, List<String> baseSources) {
        String error = null;
        if (baseSources.size() != 1) {
            error = "Match only one result at a time";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        return true;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return MatchTransformerConfig.class;
    }

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir, TransformStep step) {
        Source[] baseSources = step.getBaseSources();
        List<String> baseSourceVersions = step.getBaseVersions();
        String confStr = step.getConfig();
        String sourceDirInHdfs;
        MatchCommand matchCommand;
        MatchTransformerConfig transformerConfig = getConfiguration(confStr);
        if (!(baseSources[0] instanceof TableSource)) {
            sourceDirInHdfs = hdfsPathBuilder
                    .constructTransformationSourceDir(baseSources[0], baseSourceVersions.get(0)).toString();
            matchCommand = match(sourceDirInHdfs, workflowDir, transformerConfig);
        } else {
            TableSource tableSource = (TableSource) baseSources[0];
            Table table = (tableSource).getTable();

            if (table.getExtracts().size() != 1) {
                throw new IllegalArgumentException("Can only handle single extract table.");
            }

            String avroDir = table.getExtracts().get(0).getPath();
            String tableName = table.getName();
            CustomerSpace customerSpace = tableSource.getCustomerSpace();

            // M29: in PA, the provided table avsc might not be updated.
            // because we often update table schema after the transformation pipeline
//            Schema schema = null;
//            try {
//                String avscPath = hdfsPathBuilder //
//                        .constructTableSchemaFilePath(tableName, customerSpace, "").toString();
//                InputStream is = HdfsUtils.getInputStream(yarnConfiguration, avscPath);
//                schema = new Schema.Parser().parse(is);
//            } catch (IOException e) {
//                log.warn("Failed to get schema from avsc", e);
//            }
//            if (schema == null) {
//                table = metadataProxy.getTable(customerSpace.toString(), tableName);
//                schema = TableUtils.createSchema("input", table);
//            }

            table = metadataProxy.getTable(customerSpace.toString(), tableName);
            Schema schema = TableUtils.createSchema("input", table);
            matchCommand = match(avroDir, schema, workflowDir, transformerConfig);
        }
        if (matchCommand == null) {
            return false;
        } else {
            step.setCount(matchCommand.getRowsRequested().longValue());
            return true;
        }
    }

    abstract MatchCommand match(String inputAvroPath, String outputAvroPath, MatchTransformerConfig config);

    abstract MatchCommand match(String inputAvroPath, Schema schema, String outputAvroPath,
            MatchTransformerConfig config);
}
