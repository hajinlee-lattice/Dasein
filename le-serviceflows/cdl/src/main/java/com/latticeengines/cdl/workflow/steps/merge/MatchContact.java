package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ContactNameConcatenateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;

@Component(MatchContact.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchContact extends BaseSingleEntityMergeImports<ProcessContactStepConfiguration> {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(MatchContact.class);

    static final String BEAN_NAME = "matchContact";

    private String matchTargetTablePrefix = null;
    private String newAccountTableName = NamingUtils.timestamp("NewAccountsFromContact");

    @Value("${cdl.pa.contact.match.ignore.domain}")
    private boolean ignoreDomainMatchKeyInContact;

    @Inject
    private MatchProxy matchProxy;

    private String matchRootOperationUid;

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        if (isShortCutMode()) {
            log.info("Found entity match checkpoint in the context, using short-cut pipeline");
            return null;
        } else if (hasNoImportAndNoBatchStore()) {
            log.info("no Import and no batchStore, skip this step.");
            return null;
        } else {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MatchContact");
            matchTargetTablePrefix = entity.name() + "_Matched";
            if (configuration.isEntityMatchEnabled()) {
                bumpEntityMatchStagingVersion();
                request.setSteps(entityMatchSteps());
            } else {
                request.setSteps(legacyMatchSteps());
            }
            return request;
        }
    }

    @Override
    protected void onPostTransformationCompleted() {
        String targetTableName = getEntityMatchTargetTableName();
        mergeInputSchema(targetTableName);
        putStringValueInContext(ENTITY_MATCH_CONTACT_TARGETTABLE, targetTableName);
        addToListInContext(TEMPORARY_CDL_TABLES, targetTableName, String.class);

        Table newAccountTable = metadataProxy.getTableSummary(customerSpace.toString(), newAccountTableName);
        log.info("match RootOperationUID = {}, newAccountTableName = {}, table exists = {}", matchRootOperationUid,
                newAccountTableName, newAccountTable != null);
        if (matchRootOperationUid != null && newAccountTable != null) {
            MatchCommand cmd = matchProxy.bulkMatchStatus(matchRootOperationUid);
            Preconditions.checkNotNull(cmd,
                    String.format("Failed to retrieve match command for RootOperationUID %s", matchRootOperationUid));
            log.info("Number of newly allocated entities = {}, RootOperationUID = {}", cmd.getNewEntityCounts(),
                    matchRootOperationUid);
            if (MatchUtils.hasNewEntity(cmd.getNewEntityCounts(), BusinessEntity.Account.name())) {
                putStringValueInContext(ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE, newAccountTableName);
                addToListInContext(TEMPORARY_CDL_TABLES, newAccountTableName, String.class);
            }
        }
    }

    private boolean isShortCutMode() {
        return Boolean.TRUE.equals(getObjectFromContext(ENTITY_MATCH_COMPLETED, Boolean.class));
    }

    private List<TransformationStepConfig> entityMatchSteps() {
        List<TransformationStepConfig> steps = new ArrayList<>();
        rematchInputTableNames = ListUtils.emptyIfNull(getConvertedRematchTableNames());
        boolean isRematch = CollectionUtils.isNotEmpty(rematchInputTableNames);
        log.info("in rematch mode = {}", isRematch);
        Pair<String[][], String[][]> preProcessFlds = getPreProcessFlds(isRematch);

        if (CollectionUtils.isNotEmpty(inputTableNames)) {
            TransformationStepConfig mergeImport = concatImports(null, preProcessFlds.getLeft(),
                    preProcessFlds.getRight(), null, -1);
            steps.add(mergeImport);
            TransformationStepConfig concatenateImportContactName = concatenateContactName(steps.size() - 1, null);
            steps.add(concatenateImportContactName);
            if (CollectionUtils.isNotEmpty(rematchInputTableNames)) {
                // only try to lookup existing account ID for imports, don't care about new
                // account since later match step will take care of that
                TransformationStepConfig entityMatchImport = matchContact(steps.size() - 1, null, null, false);
                steps.add(entityMatchImport);
            }
        }
        if (CollectionUtils.isNotEmpty(rematchInputTableNames)) {
            // when there is no input table, steps.size() - 1 will be -1
            TransformationStepConfig mergeBatchStore = concatImports(null, preProcessFlds.getLeft(),
                    preProcessFlds.getRight(), rematchInputTableNames, steps.size() - 1);
            steps.add(mergeBatchStore);
            TransformationStepConfig concatenateBatchStoreContactName = concatenateContactName(steps.size() - 1, null);
            steps.add(concatenateBatchStoreContactName);
            // If has rematch fake imports, filter out those columns after concat all imports
            TransformationStepConfig filterImports = filterColumnsFromImports(steps.size() - 1);
            steps.add(filterImports);
        }
        TransformationStepConfig entityMatch = matchContact(steps.size() - 1, matchTargetTablePrefix,
                rematchInputTableNames, true);
        steps.add(entityMatch);
        log.info("steps are {}.", steps);
        return steps;
    }

    private List<TransformationStepConfig> legacyMatchSteps() {
        List<TransformationStepConfig> steps = new ArrayList<>();
        int mergeStep = 0;
        TransformationStepConfig merge = dedupAndConcatImports(InterfaceName.ContactId.name());
        TransformationStepConfig concatenate = concatenateContactName(mergeStep, matchTargetTablePrefix);
        steps.add(merge);
        steps.add(concatenate);
        return steps;
    }

    private TransformationStepConfig concatenateContactName(int mergeStep, String targetTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(mergeStep));
        if (targetTableName != null) {
            setTargetTable(step, targetTableName);
        }
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONTACT_NAME_CONCATENATER);
        ContactNameConcatenateConfig config = new ContactNameConcatenateConfig();
        config.setConcatenateFields(new String[] { InterfaceName.FirstName.name(), InterfaceName.LastName.name() });
        config.setResultField(InterfaceName.ContactName.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step;
    }

    private TransformationStepConfig matchContact(int inputStep, String targetTableName,
            List<String> convertedRematchTableNames, boolean registerNewAccountTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        setTargetTable(step, targetTableName);
        step.setTransformer(TRANSFORMER_MATCH);
        String configStr;
        // combine columns from all imports
        Set<String> columnNames = getInputTableColumnNames();
        boolean hasConvertedRematchTables = CollectionUtils.isNotEmpty(convertedRematchTableNames);
        MatchInput matchInput = getBaseMatchInput();
        if (hasConvertedRematchTables) {
            convertedRematchTableNames.forEach(tableName -> {
                columnNames.addAll(getTableColumnNames(tableName));
            });

            setRematchVersions(matchInput);
        }
        if (configuration.getEntityMatchConfiguration() != null) {
            log.info("found custom entity match configuration = {}", configuration.getEntityMatchConfiguration());
            matchInput.setEntityMatchConfiguration(configuration.getEntityMatchConfiguration());
        }
        log.info("Ignore domain match keys for account in contact = {}", ignoreDomainMatchKeyInContact);
        if (configuration.isEntityMatchGAOnly()) {
            configStr = MatchUtils.getAllocateIdMatchConfigForContact(customerSpace.toString(), matchInput, columnNames,
                    getSystemIds(BusinessEntity.Account), getSystemIds(BusinessEntity.Contact), null,
                    hasConvertedRematchTables, ignoreDomainMatchKeyInContact, null);
        } else {
            matchRootOperationUid = UUID.randomUUID().toString();
            configStr = MatchUtils.getAllocateIdMatchConfigForContact(customerSpace.toString(), matchInput, columnNames,
                    getSystemIds(BusinessEntity.Account), getSystemIds(BusinessEntity.Contact),
                    registerNewAccountTable ? newAccountTableName : null,
                    hasConvertedRematchTables, ignoreDomainMatchKeyInContact, matchRootOperationUid);
            log.info("Set match RootOperationUID to {}", matchRootOperationUid);
        }
        step.setConfiguration(configStr);
        return step;
    }

    private String getEntityMatchTargetTableName() {
        return TableUtils.getFullTableName(matchTargetTablePrefix, pipelineVersion);
    }

    /**
     * For PA during entity match migration period: some files are imported with
     * legacy template (having ContactId/AccountId) while some files are imported
     * after template is upgraded (having CustomerContactId/CustomerAccountId)
     *
     * It's to rename ContactId/AccountId to CustomerContactId/CustomerAccountId and
     * copy to DefaultSystem's ID with same value
     *
     * Copy happens before rename and the merge job has check whether specified
     * original column (ContactId/AccountId) exists or not
     *
     * TODO: After all the tenants finish entity match migration, we could get rid
     * of this field rename/copy logic
     *
     * @return <cloneFlds, renameFlds>
     */
    private Pair<String[][], String[][]> getPreProcessFlds(boolean isRematch) {
        if (isRematch) {
            // don't clone/rename in rematch, otherwise occount id generated by us will
            // polute customer's ID
            return Pair.of(null, null);
        }
        String defaultAcctSysId = getDefaultSystemId(BusinessEntity.Account);
        String defaultContSysId = getDefaultSystemId(BusinessEntity.Contact);
        List<String[]> cloneFldList = new ArrayList<>();
        if (defaultAcctSysId != null) {
            cloneFldList.add(new String[] { InterfaceName.AccountId.name(), defaultAcctSysId });
        }
        if (defaultContSysId != null) {
            cloneFldList.add(new String[] { InterfaceName.ContactId.name(), defaultContSysId });
        }
        String[][] cloneFlds = cloneFldList.isEmpty() ? null : cloneFldList.toArray(new String[0][]);
        String[][] renameFlds = { //
                { InterfaceName.AccountId.name(), InterfaceName.CustomerAccountId.name() }, //
                { InterfaceName.ContactId.name(), InterfaceName.CustomerContactId.name() } //
        };
        return Pair.of(cloneFlds, renameFlds);
    }

}
