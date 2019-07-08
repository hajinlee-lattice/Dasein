package com.latticeengines.cdl.workflow.steps.update;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component(ProcessAccountDiff.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessAccountDiff extends BaseProcessSingleEntityDiffStep<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProcessAccountDiff.class);

    static final String BEAN_NAME = "processAccountDiff";

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    private String enrichedTablePrefix = "EnrichedAccountDiff";
    private boolean shortCutMode = false;
    private Table enrichedDiffTable;

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        String enrichedDiffTableName = getStringValueFromContext(ENRICHED_ACCOUNT_DIFF_TABLE_NAME);
        if (StringUtils.isNotBlank(enrichedDiffTableName)) {
            enrichedDiffTable = metadataProxy.getTable(customerSpace.toString(), enrichedDiffTableName);
            if (enrichedDiffTable != null) {
                log.info("Found enriched account diff table in context, going thru short-cut mode.");
                shortCutMode = true;
            }
        }

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ConsolidateAccountDiff");
        if (shortCutMode) {
            request.setSteps(shortCutSteps());
        } else {
            request.setSteps(regularSteps());
        }
        return request;
    }

    private List<TransformationStepConfig> regularSteps() {
        int step = 0;
        int matchStep = step++;
        int bucketStep = step++;
        TransformationStepConfig matchDiff = match();
        TransformationStepConfig bucket = bucket(matchStep, true);
        TransformationStepConfig retainFields = retainFields(bucketStep);
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(matchDiff);
        steps.add(bucket);
        steps.add(retainFields);
        return steps;
    }

    private List<TransformationStepConfig> shortCutSteps() {
        diffTableName = enrichedDiffTable.getName();
        int bucketStep = 0;
        TransformationStepConfig bucket = bucket( true);
        TransformationStepConfig retainFields = retainFields(bucketStep);
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(bucket);
        steps.add(retainFields);
        return steps;
    }


    @Override
    protected TableRoleInCollection profileTableRole() {
        return TableRoleInCollection.Profile;
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
        String enrichedDiffTableName;
        if (shortCutMode) {
            enrichedDiffTableName = enrichedDiffTable.getName();
        } else {
            enrichedDiffTableName = TableUtils.getFullTableName(enrichedTablePrefix, pipelineVersion);
            enrichedDiffTable = metadataProxy.getTable(customerSpace.toString(), enrichedDiffTableName);
            if (enrichedDiffTable == null) {
                throw new RuntimeException(
                        "Failed to find enriched account diff table " + enrichedDiffTableName + " in customer " + customerSpace);
            }
            exportToS3AndAddToContext(enrichedDiffTableName, ENRICHED_ACCOUNT_DIFF_TABLE_NAME);
        }
        addToListInContext(TEMPORARY_CDL_TABLES, enrichedDiffTableName, String.class);
    }

    private TransformationStepConfig match() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MATCH);
        addBaseTables(step, diffTableName);

        // Match Input
        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(customerSpace.toString()));

        String dataCloudVersion = "";
        DataCollectionStatus detail = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (detail != null) {
            try {
                dataCloudVersion = DataCloudVersion.parseBuildNumber(detail.getDataCloudBuildNumber()).getVersion();
            } catch (Exception e) {
                log.warn("Failed to read datacloud version from collection status " + JsonUtils.serialize(detail));
            }
        }

        List<ColumnMetadata> dcCols = columnMetadataProxy.getAllColumns(dataCloudVersion);
        List<Column> cols = new ArrayList<>();
        for (ColumnMetadata cm : dcCols) {
            cols.add(new Column(cm.getAttrName()));
        }
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(cols);
        matchInput.setCustomSelection(cs);

        Map<MatchKey, List<String>> keyMap = new TreeMap<>();
        keyMap.put(MatchKey.LatticeAccountID, Collections.singletonList(InterfaceName.LatticeAccountId.name()));
        matchInput.setKeyMap(keyMap);

        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(true);
        matchInput.setFetchOnly(true);
        matchInput.setSplitsPerBlock(cascadingPartitions * 10);
        config.setMatchInput(matchInput);

        setTargetTable(step, enrichedTablePrefix);

        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }
}
