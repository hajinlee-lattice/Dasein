package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ContactNameConcatenateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

@Component(MergeContact.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeContact extends BaseSingleEntityMergeImports<ProcessContactStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeContact.class);

    static final String BEAN_NAME = "mergeContact";

    private int mergeStep;
    private int concatenateStep;
    private int entityMatchStep;
    private int upsertMasterStep;
    private int diffStep;

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MergeContact");

            boolean entityMatchEnabled = configuration.isEntityMatchEnabled();

            int stepCount = 0;
            mergeStep = stepCount++;
            concatenateStep = stepCount++;
            if (entityMatchEnabled) {
                entityMatchStep = stepCount++;
            }
            upsertMasterStep = stepCount++;
            diffStep = stepCount;

            TransformationStepConfig merge = mergeInputs(false, true, false);
            TransformationStepConfig concatenate = concatenateContactName(mergeStep);
            TransformationStepConfig entityMatch = null;
            TransformationStepConfig upsertMaster;
            if (entityMatchEnabled) {
                // add bulk match between concat & merge master to perform lead to account match
                entityMatch = leadToAccountMatch(concatenateStep);
                upsertMaster = mergeMaster(entityMatchStep);
            } else {
                upsertMaster = mergeMaster(concatenateStep);
            }
            TransformationStepConfig diff = diff(concatenateStep, upsertMasterStep);
            TransformationStepConfig report = reportDiff(diffStep);

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(merge);
            steps.add(concatenate);
            if (entityMatchEnabled) {
                steps.add(entityMatch);
            }
            steps.add(upsertMaster);
            steps.add(diff);
            steps.add(report);
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig concatenateContactName(int mergeStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(mergeStep));
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONTACT_NAME_CONCATENATER);
        ContactNameConcatenateConfig config = new ContactNameConcatenateConfig();
        config.setConcatenateFields(new String[] { InterfaceName.FirstName.name(), InterfaceName.LastName.name() });
        config.setResultField(InterfaceName.ContactName.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step;
    }

    @Override
    protected void enrichTableSchema(Table table) {
        if (!configuration.isEntityMatchEnabled()) {
            return;
        }

        table.getAttributes().forEach(attr -> {
            // update metadata for AccountId attribute since it is only created after lead
            // to account match and does not have the correct metadata
            if (InterfaceName.AccountId.name().equals(attr.getName())) {
                attr.setInterfaceName(InterfaceName.AccountId);
                attr.setTags(Tag.INTERNAL);
                attr.setLogicalDataType(LogicalDataType.Id);
                attr.setNullable(false);
                attr.setApprovedUsage(ApprovedUsage.NONE);
                attr.setSourceLogicalDataType(attr.getPhysicalDataType());
                attr.setCategory(Category.CONTACT_ATTRIBUTES.name());
                attr.setAllowedDisplayNames(
                        Arrays.asList("ACCOUNT_ID", "ACCOUNTID", "ACCOUNT_EXTERNAL_ID", "ACCOUNT ID", "ACCOUNT"));
                attr.setFundamentalType(FundamentalType.ALPHA.getName());
            }
        });
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
    }

    /*
     * Lead to account match configurations
     */

    private TransformationStepConfig leadToAccountMatch(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        step.setTransformer(TRANSFORMER_MATCH);
        MatchTransformerConfig config = new MatchTransformerConfig();
        config.setMatchInput(getMatchInput());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private MatchInput getMatchInput() {
        MatchInput input = getBaseMatchInput();
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setSkipKeyResolution(true);
        input.setTargetEntity(BusinessEntity.Account.name());
        // lookup mode for lead to account match
        input.setAllocateId(false);
        input.setFetchOnly(false);
        input.setPredefinedSelection(ColumnSelection.Predefined.LeadToAcct);
        MatchInput.EntityKeyMap entityKeyMap = new MatchInput.EntityKeyMap();
        entityKeyMap.setKeyMap(getMatchKeys());
        input.setEntityKeyMaps(Collections.singletonMap(BusinessEntity.Account.name(), entityKeyMap));
        return input;
    }

    private Map<MatchKey, List<String>> getMatchKeys() {
        Set<String> columnNames = getInputTableColumnNames(0);
        // email is a required for lead to account match
        Preconditions.checkArgument(columnNames.contains(InterfaceName.Email.name()),
                "Missing mandatory column (Email) in input table for lead to account match");
        Map<MatchKey, List<String>> matchKeys = new HashMap<>();
        // for domain match key, value in Email column is considered first, if no value
        // for Email, try to use value in Website column instead (set in
        // addLDCMatchKeysIfExist)
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.Domain, InterfaceName.Email.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.SystemId, InterfaceName.CustomerAccountId.name());
        addLDCMatchKeysIfExist(columnNames, matchKeys);
        log.info("Lead to account match keys = {}", JsonUtils.serialize(matchKeys));
        return matchKeys;
    }
}
