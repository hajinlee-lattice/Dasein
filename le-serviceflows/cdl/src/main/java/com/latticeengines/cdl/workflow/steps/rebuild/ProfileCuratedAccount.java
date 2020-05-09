package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.CalculatedCuratedAccountAttribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedAccountAttributesStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

// Description: Runs a Workflow Step to compute "curated" attributes which are derived from other attributes.  At this
//     time the only curated attributes is the Number of Contacts per account.  This computation employs the
//     Transformation framework.
@Component(ProfileCuratedAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileCuratedAccount extends BaseSingleEntityProfileStep<CuratedAccountAttributesStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProfileCuratedAccount.class);

    public static final String BEAN_NAME = "profileCuratedAccount";

    // Set to true if no serving store table generated
    private boolean skipTransformation;

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.CuratedAccount;
    }

    @Override
    protected TableRoleInCollection profileTableRole() {
        return null;
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        // Initially, plan to run this step's transformation.
        skipTransformation = false;

        initializeConfiguration();

        // Only generate a Workflow Configuration if all the necessary input tables are
        // available and the
        // step's configuration.
        if (skipTransformation) {
            return null;
        } else {
            return generateWorkflowConf();
        }
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();

        List<Table> tablesInCtx = getTableSummariesFromCtxKeys(customerSpace.toString(), //
                Arrays.asList(CURATED_ACCOUNT_SERVING_TABLE_NAME, CURATED_ACCOUNT_STATS_TABLE_NAME));
        boolean shortCut = tablesInCtx.stream().noneMatch(Objects::isNull);

        if (shortCut) {
            log.info("Found both serving and stats tables in workflow context, going thru short-cut mode.");
            servingStoreTableName = renameServingStoreTable(tablesInCtx.get(0));
            statsTableName = tablesInCtx.get(1).getName();

            TableRoleInCollection servingStoreRole = BusinessEntity.CuratedAccount.getServingStore();
            dataCollectionProxy.upsertTable(customerSpace.toString(), servingStoreTableName, //
                    servingStoreRole, inactive);
            exportTableRoleToRedshift(servingStoreTableName, servingStoreRole);
            updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);

            skipTransformation = true;

            finishing();
        } else {
            setEvaluationDateStrAndTimestamp();
            if (tablesInCtx.get(0) == null) {
                log.info("No serving table found, skip profiling");
                skipTransformation = true;
                return;
            }

            servingStoreTableName = renameServingStoreTable(tablesInCtx.get(0));
            log.info("Set serving table name to value in ctx = {}", servingStoreTableName);
            if (!skipTransformation) {
                double servingTableSize = ScalingUtils.getTableSizeInGb(yarnConfiguration, tablesInCtx.get(0));
                int multiplier = ScalingUtils.getMultiplier(servingTableSize);
                log.info("Set scalingMultiplier={} base on curated account table size={} gb", multiplier,
                        servingTableSize);
                scalingMultiplier = multiplier;
            }
        }
    }

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();

        request.setName("ProfileCuratedAccount");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        int profileStep = 0;
        int bucketStep = 1;

        // PBS
        TransformationStepConfig profile = profile(servingStoreTableName);
        TransformationStepConfig calcStats = calcStats(profileStep, servingStoreTableName, statsTablePrefix);
        steps.add(profile);
        steps.add(calcStats);

        // -----------
        request.setSteps(steps);
        return request;
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
        exportToS3AndAddToContext(statsTableName, CURATED_ACCOUNT_STATS_TABLE_NAME);
        finishing();
    }

    private void finishing() {
        updateDCStatusForCuratedAccountAttributes();
        TableRoleInCollection role = CalculatedCuratedAccountAttribute;
        exportToDynamo(servingStoreTableName, role.getPartitionKey(), role.getRangeKey());
    }

    protected void updateDCStatusForCuratedAccountAttributes() {
        // Get the data collection status map and set the last data refresh time for
        // Curated Accounts to the more
        // recent of the data collection times of Accounts and Contacts.
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        Map<String, Long> dateMap = status.getDateMap();
        if (MapUtils.isEmpty(dateMap)) {
            log.error("No data in DataCollectionStatus Date Map despite running Curated Account Attributes step");
        } else {
            Long accountCollectionTime = 0L;
            if (dateMap.containsKey(Category.ACCOUNT_ATTRIBUTES.getName())) {
                accountCollectionTime = dateMap.get(Category.ACCOUNT_ATTRIBUTES.getName());
            }
            Long contactCollectionTime = 0L;
            if (dateMap.containsKey(Category.CONTACT_ATTRIBUTES.getName())) {
                contactCollectionTime = dateMap.get(Category.CONTACT_ATTRIBUTES.getName());
            }
            long curatedAccountCollectionTime = Long.max(accountCollectionTime, contactCollectionTime);
            if (curatedAccountCollectionTime == 0L) {
                log.error("No Account or Contact DataCollectionStatus dates despite running Curated Account "
                        + "Attributes step");
            } else {
                dateMap.put(Category.CURATED_ACCOUNT_ATTRIBUTES.getName(), curatedAccountCollectionTime);
                putObjectInContext(CDL_COLLECTION_STATUS, status);
            }
        }
    }
}
