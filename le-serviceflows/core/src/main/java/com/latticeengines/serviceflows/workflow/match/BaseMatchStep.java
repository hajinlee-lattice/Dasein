package com.latticeengines.serviceflows.workflow.match;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.ReflectionUtils;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class BaseMatchStep<S extends BaseStepConfiguration> extends BaseWorkflowStep<S> {

    private static final Logger log = LoggerFactory.getLogger(BaseMatchStep.class);

    static final Map<MatchKey, String> MATCH_KEYS_TO_DISPLAY_NAMES = new HashMap<>();
    static {
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.Name, InterfaceName.CompanyName.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.City, InterfaceName.City.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.State, InterfaceName.State.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.Country, InterfaceName.Country.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.Zipcode, InterfaceName.PostalCode.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.PhoneNumber, InterfaceName.PhoneNumber.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.DUNS, InterfaceName.DUNS.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.ExternalId, InterfaceName.Id.name());
    }

    @Inject
    private BulkMatchService bulkMatchService;

    @Inject
    protected Configuration yarnConfiguration;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    private CustomerSpace customerSpace;

    @Override
    public void execute() {
        log.info("Executing spark step " + getClass().getSimpleName());
        customerSpace = parseCustomerSpace(configuration);
        String inputAvroPath = getInputAvroPath();
        MatchInput input = constructMatchInput(inputAvroPath);
        preMatchProcessing(input);
        if (!OperationalMode.isEntityMatch(input.getOperationalMode())) {
            input.setDataCloudOnly(true);
        }
        MatchCommand command = bulkMatchService.match(input, getPredeterminedRootOperationUid());
        log.info("Bulk match finished: {}", JsonUtils.serialize(command));
        postMatchProcessing(input, command);
    }

    protected abstract String getInputAvroPath();
    protected abstract String getResultTableName();
    protected abstract void preMatchProcessing(MatchInput matchInput);

    protected void postMatchProcessing(MatchInput input, MatchCommand command) {
        String customer = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
        String resultTableName = getResultTableName();
        bulkMatchService.registerResultTable(customer, command, resultTableName);
        putStringValueInContext(MATCH_RESULT_TABLE_NAME, resultTableName);

        String newEntitiesTableName = getNewEntitiesTableName();
        if (StringUtils.isNotBlank(newEntitiesTableName) && input.isOutputNewEntities()) {
            bulkMatchService.registerNewEntitiesTable(customer, command, newEntitiesTableName);
        }
    }

    protected Map<MatchKey, String> getPotentialMatchKeyMapping() {
        return new HashMap<>(MATCH_KEYS_TO_DISPLAY_NAMES);
    }

    protected String getPredeterminedRootOperationUid() {
        return null;
    }

    protected String getNewEntitiesTableName() {
        return null;
    }

    protected CustomerSpace parseCustomerSpace(S stepConfiguration) {
        if (stepConfiguration instanceof SparkJobStepConfiguration) {
            SparkJobStepConfiguration sparkJobStepConfiguration = (SparkJobStepConfiguration) stepConfiguration;
            return CustomerSpace.parse(sparkJobStepConfiguration.getCustomer());
        } else {
            Method method = ReflectionUtils.findMethod(stepConfiguration.getClass(), "getCustomerSpace");
            if (method != null) {
                if (CustomerSpace.class.equals(method.getReturnType())) {
                    return (CustomerSpace) ReflectionUtils.invokeMethod(method, stepConfiguration);
                } else if (String.class.equals(method.getReturnType())) {
                    String customerSpaceStr = (String) ReflectionUtils.invokeMethod(method, stepConfiguration);
                    return CustomerSpace.parse(customerSpaceStr);
                }
            }
            throw new UnsupportedOperationException("Do not know how to parse customer space from a " //
                    + stepConfiguration.getClass().getCanonicalName());
        }
    }

    private MatchInput constructMatchInput(String avroDir) {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setOperationalMode(OperationalMode.LDC_MATCH);

        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        matchInput.setInputBuffer(inputBuffer);

        Map<MatchKey, String> potentialKeyMap = getPotentialMatchKeyMapping();
        Set<String> inputFields = getInputFields(avroDir);
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        potentialKeyMap.forEach((key, col) -> {
            if (inputFields.contains(col)) {
                keyMap.put(key, Collections.singletonList(col));
            }
        });
        matchInput.setKeyMap(keyMap);
        matchInput.setSkipKeyResolution(true);
        return matchInput;
    }

    private Set<String> getInputFields(String avroDir) {
        String avroGlob = PathUtils.toAvroGlob(avroDir);
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        return schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
    }

}
