package com.latticeengines.datacloud.match.testframework;

import static org.apache.commons.collections4.CollectionUtils.isEmpty;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.testng.Assert;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Base test class for entity match functional test targeting
 * {@link RealTimeMatchService#match(MatchInput)} interface
 */
public abstract class EntityMatchFunctionalTestNGBase extends DataCloudMatchFunctionalTestNGBase {

    @Inject
    protected RealTimeMatchService realTimeMatchService;

    @Inject
    protected TestEntityMatchService testEntityMatchService;

    @Inject
    protected EntityMatchInternalService entityMatchInternalService;

    @Inject
    protected EntityMatchConfigurationService entityMatchConfigurationService;

    /*
     * create a new test tenant
     */
    protected Tenant newTestTenant() {
        return new Tenant(createNewTenantId());
    }

    protected String createNewTenantId() {
        return getClass().getSimpleName() + "_" + UUID.randomUUID().toString();
    }

    // This function is useful for debugging tests. It prints out the input fields
    // and data and then the output
    // with the match logs and error messages nicely formatted. logLevel must be set
    // to "DEBUG" in the MatchInput for
    // this function to work properly, eg. matchInput.setLogLevelEnum(Level.DEBUG);
    protected void logAndVerifyMatchLogsAndErrors(List<String> fieldList, List<Object> dataList,
            MatchOutput matchOutput, List<String> expectedErrorMsgs) {
        StringBuilder msg = new StringBuilder("Test Case Results:");
        String fields = String.join(" ", fieldList);
        msg.append("\nFields:\n  ").append(fields);
        String data = dataList.stream().map(x -> x == null ? "null" : x.toString()).collect(Collectors.joining(" "));
        msg.append("\nData:\n  ").append(data);

        List<OutputRecord> outputRecordList = matchOutput.getResult();
        for (OutputRecord outputRecord : outputRecordList) {
            msg.append("\nOutput Record row: ").append(outputRecord.getRowNumber()).append(" matched: ")
                    .append(outputRecord.isMatched()).append(" Entity ID: ")
                    .append(getColumnValue(matchOutput, InterfaceName.EntityId.name()));

            if (CollectionUtils.isNotEmpty(outputRecord.getMatchLogs())) {
                String matchLog = String.join("\n  ", outputRecord.getMatchLogs());
                msg.append("\nLogs:\n  ").append(matchLog);
            } else {
                msg.append("\nNo Logs");
            }

            if (CollectionUtils.isNotEmpty(outputRecord.getErrorMessages())) {
                String matchErrors = String.join("\n  ", outputRecord.getErrorMessages());
                msg.append("\nErrors:\n  ").append(matchErrors);
            } else {
                msg.append("\nNo Errors");
            }
            if (CollectionUtils.isNotEmpty(expectedErrorMsgs)) {
                Assert.assertEquals(String.join(" && ", outputRecord.getErrorMessages()),
                        String.join(" && ", expectedErrorMsgs));
            } else {
                Assert.assertTrue(isEmpty(outputRecord.getErrorMessages()));
            }
        }
        getLogger().info(msg.toString());
    }

    /*
     * make sure that match output has exactly one row that only contains the
     * expected output columns, first column should be entityId and second column
     * should be the target entity ID (e.g., AccountId)
     */
    protected String verifyAndGetEntityId(@NotNull MatchOutput output, String targetEntityIdColumn) {
        Assert.assertNotNull(output);
        Assert.assertNotNull(output.getResult());
        Assert.assertEquals(output.getResult().size(), 1);
        OutputRecord record = output.getResult().get(0);
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getOutput());
        List<String> expectedOutputColumns = getExpectedOutputColumns();
        Assert.assertEquals(output.getOutputFields(), expectedOutputColumns);
        Assert.assertEquals(record.getOutput().size(), expectedOutputColumns.size());
        for (int i = 0; i < expectedOutputColumns.size(); i++) {
            if (record.getOutput().get(i) != null) {
                Assert.assertTrue(record.getOutput().get(i) instanceof String);
            }
        }
        String entityId = getColumnValue(output, InterfaceName.EntityId.name());
        Assert.assertNotNull(entityId);
        if (targetEntityIdColumn != null) {
            // EntityId and {TargetEntity}Id should have same value
            Assert.assertEquals(getColumnValue(output, targetEntityIdColumn), entityId);
        }
        if (DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(entityId)) {
            Assert.assertFalse(record.isMatched());
        } else {
            Assert.assertTrue(record.isMatched());
        }
        return entityId;
    }

    /*
     * Get the value of specified column in the FIRST record, return null if there
     * is no such column
     */
    protected String getColumnValue(MatchOutput output, String columnName) {
        if (output == null || isEmpty(output.getResult()) || isEmpty(output.getOutputFields())) {
            return null;
        }

        OutputRecord record = output.getResult().get(0);
        if (record == null || record.getOutput() == null) {
            return null;
        }

        List<String> fields = output.getOutputFields();
        for (int i = 0; i < fields.size(); i++) {
            if (columnName.equals(fields.get(i))) {
                return (String) record.getOutput().get(i);
            }
        }
        return null;
    }

    /*
     * helper to prepare basic MatchInput for entity match
     */
    protected MatchInput prepareEntityMatchInput(@NotNull Tenant tenant, @NotNull String targetEntity,
            @NotNull Map<String, MatchInput.EntityKeyMap> entityKeyMaps) {
        MatchInput input = new MatchInput();

        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setTenant(tenant);
        input.setTargetEntity(targetEntity);
        // only support this predefined selection for now
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setEntityKeyMaps(entityKeyMaps);
        input.setDataCloudVersion(currentDataCloudVersion);
        input.setSkipKeyResolution(true);
        input.setUseRemoteDnB(true);
        input.setUseDnBCache(true);
        input.setRootOperationUid(UUID.randomUUID().toString());
        // Enable debug logs for Match Traveler.
        input.setLogLevelEnum(Level.DEBUG);

        return input;
    }

    /*
     * publish data for input list of entities from staging to serving
     */
    protected List<EntityPublishStatistics> publishToServing(@NotNull Tenant tenant, BusinessEntity... entities) {
        return Arrays.stream(entities) //
                .map(entity -> entityMatchInternalService.publishEntity(entity.name(), tenant, tenant,
                        EntityMatchEnvironment.SERVING, true)) //
                .collect(Collectors.toList());
    }

    /*
     * Return a list of expected column names in match output that will be used by
     * verifyAndGetEntityId
     */
    protected abstract List<String> getExpectedOutputColumns();

    protected abstract Logger getLogger();
}
