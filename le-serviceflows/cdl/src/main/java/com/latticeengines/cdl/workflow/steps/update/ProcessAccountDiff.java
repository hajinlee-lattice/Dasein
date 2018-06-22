package com.latticeengines.cdl.workflow.steps.update;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component(ProcessAccountDiff.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessAccountDiff extends BaseProcessSingleEntityDiffStep<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProcessAccountDiff.class);

    static final String BEAN_NAME = "processAccountDiff";

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ConsolidateAccountDiff");

        int matchStep = 0;
        int bucketStep = 1;
        int retainStep = 2;

        TransformationStepConfig matchDiff = match();
        TransformationStepConfig bucket = bucket(matchStep, true);
        TransformationStepConfig retainFields = retainFields(bucketStep, false);
        TransformationStepConfig sort = sort(retainStep, 200);

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(matchDiff);
        steps.add(bucket);
        steps.add(retainFields);
        steps.add(sort);
        request.setSteps(steps);
        return request;
    }

    @Override
    protected TableRoleInCollection profileTableRole() {
        return TableRoleInCollection.Profile;
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
        registerDynamoExport();
    }

    private TransformationStepConfig match() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MATCH);

        useDiffTableAsSource(step);

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

        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private void registerDynamoExport() {
        String masterTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedAccount, inactive);
        exportToDynamo(diffTableName, masterTableName, InterfaceName.AccountId.name(), null);
    }
}
