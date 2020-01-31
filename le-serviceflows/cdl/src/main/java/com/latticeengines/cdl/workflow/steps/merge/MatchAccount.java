package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(MatchAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchAccount extends BaseSingleEntityMergeImports<ProcessAccountStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(MatchAccount.class);

    static final String BEAN_NAME = "matchAccount";

    private String matchTargetTablePrefix = null;

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("MatchAccount");
        matchTargetTablePrefix = entity.name() + "_Matched";

        if (isShortCutMode()) {
            log.info("Found diff table and batch store in context, using short-cut pipeline");
            return null;
        }

        if (hasNoImportAndNoBatchStore()) {
            log.info("no Import and no batchStore, skip this step.");
            return null;
        }

        List<TransformationStepConfig> steps = new ArrayList<>();
        String convertBatchStoreTableName = getConvertBatchStoreTableName();
        if (configuration.isEntityMatchEnabled()) {
            bumpEntityMatchStagingVersion();
            Pair<String[][], String[][]> preProcessFlds = getPreProcessFlds();

            if (CollectionUtils.isNotEmpty(inputTableNames)) {
                TransformationStepConfig mergeImports = concatImports(null, preProcessFlds.getLeft(),
                        preProcessFlds.getRight(), null, -1);
                steps.add(mergeImports);
                if (StringUtils.isNotBlank(convertBatchStoreTableName)) {
                    TransformationStepConfig matchImport =  matchAccount(steps.size() - 1, null, null);
                    steps.add(matchImport);
                }
            }
            if (StringUtils.isNotBlank(convertBatchStoreTableName)) {
                TransformationStepConfig mergeBatchStoreAndImport = concatImports(null, preProcessFlds.getLeft(),
                        preProcessFlds.getRight(),
                        convertBatchStoreTableName, steps.size() - 1);
                steps.add(mergeBatchStoreAndImport);
            }
        } else {
            TransformationStepConfig merge = dedupAndConcatImports(InterfaceName.AccountId.name());
            steps.add(merge);
        }

        if (shouldExcludeDataCloudAttrs()) {
            // use latest step as output, no need to match
            TransformationStepConfig lastStep = steps.get(steps.size() - 1);
            setTargetTable(lastStep, matchTargetTablePrefix);
        } else {
            TransformationStepConfig match = matchAccount(steps.size() - 1, matchTargetTablePrefix,
                    convertBatchStoreTableName);
            steps.add(match);
        }

        log.info("steps are {}.", steps);
        request.setSteps(steps);
        return request;
    }

    /**
     * For PA during entity match migration period: some files are imported with
     * legacy template (having AccountId) while some files are imported after
     * template is upgraded (having CustomerAccountId)
     *
     * It's to rename AccountId to CustomerAccountId and copy to DefaultSystem's
     * ID with same value
     *
     * Copy happens before rename and the merge job has check whether specified
     * original column (AccountId) exists or not
     *
     * TODO: After all the tenants finish entity match migration, we could get
     * rid of this field rename/copy logic
     *
     * @return <cloneFlds, renameFlds>
     */
    private Pair<String[][], String[][]> getPreProcessFlds() {
        String defaultAcctSysId = getDefaultSystemId(entity);
        String[][] cloneFlds = defaultAcctSysId == null ? null
                : new String[][] { { InterfaceName.AccountId.name(), defaultAcctSysId } };
        String[][] renameFlds = { { InterfaceName.AccountId.name(), InterfaceName.CustomerAccountId.name() } };
        return Pair.of(cloneFlds, renameFlds);
    }

    private boolean isShortCutMode() {
        return Boolean.TRUE.equals(getObjectFromContext(ENTITY_MATCH_COMPLETED, Boolean.class));
    }

    @Override
    protected void onPostTransformationCompleted() {
        String targetTableName = TableUtils.getFullTableName(matchTargetTablePrefix, pipelineVersion);
        mergeInputSchema(targetTableName);
        putStringValueInContext(ENTITY_MATCH_ACCOUNT_TARGETTABLE, targetTableName);
        addToListInContext(TEMPORARY_CDL_TABLES, targetTableName, String.class);
    }

    private TransformationStepConfig matchAccount(int inputStep, String matchTargetTable,
            String convertBatchStoreTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        if (matchTargetTable != null) {
            setTargetTable(step, matchTargetTable);
        }
        step.setTransformer(TRANSFORMER_MATCH);
        step.setConfiguration(getMatchConfig(convertBatchStoreTableName));
        return step;
    }

    private String getMatchConfig(String convertBatchStoreTableName) {
        MatchInput matchInput = getBaseMatchInput();
        if (configuration.isEntityMatchEnabled()) {
            // combine columns from all imports
            Set<String> columnNames = getInputTableColumnNames();
            boolean hasConvertBatchStoreTableName = StringUtils.isNotEmpty(convertBatchStoreTableName);
            if (hasConvertBatchStoreTableName) {
                columnNames.addAll(getTableColumnNames(convertBatchStoreTableName));
                setRematchVersions(matchInput);
            }
            return MatchUtils.getAllocateIdMatchConfigForAccount(customerSpace.toString(), matchInput, columnNames,
                    getSystemIds(BusinessEntity.Account), null, hasConvertBatchStoreTableName);
        } else {
            // for non-entity match, schema for all imports are the same (only one
            // template). thus checking the first table is enough
            Set<String> columnNames = getInputTableColumnNames(0);
            return MatchUtils.getLegacyMatchConfigForAccount(customerSpace.toString(), matchInput, columnNames);
        }
    }



    //FIXME: a temp hotfix for M34. to be replaced by datablock implementation.
    private boolean shouldExcludeDataCloudAttrs() {
        Camille camille = CamilleEnvironment.getCamille();
        String podId = CamilleEnvironment.getPodId();
        Path node = PathBuilder.buildPodPath(podId).append("M34HotFixTargets");
        boolean shouldExclude = false;
        try {
            if (camille.exists(node)) {
                List<String> targets = Arrays.asList(camille.get(node).getData().split(","));
                String tenantId = configuration.getCustomerSpace().getTenantId();
                if (targets.contains(tenantId)) {
                    log.info("{} is a hotfix target.", tenantId);
                    shouldExclude = true;
                } else {
                    log.info("{} is not a hotfix target", tenantId);
                    shouldExclude = false;
                }
            }
        } catch (Exception e) {
            log.warn("Failed to retrieve hotfix targets from ZK.", e);
        }
        return shouldExclude;
    }

}
