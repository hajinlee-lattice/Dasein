package com.latticeengines.datacloud.dataflow.transformation.dunsredirect;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.ValidationUtils;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.AddFieldFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.AddKeyPartitionStrategy;
import com.latticeengines.dataflow.runtime.cascading.propdata.ManualSeedKeyPartitionRowFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DunsRedirectBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PrepareDunsRedirectManualMatchConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

/**
 * Generate multiple rows for different KeyPartition from ManualSeedStandard data
 */
@Component(PrepareDunsRedirectManualMatch.DATAFLOW_BEAN_NAME)
public class PrepareDunsRedirectManualMatch
        extends ConfigurableFlowBase<PrepareDunsRedirectManualMatchConfig> {
    public static final String DATAFLOW_BEAN_NAME = "PrepareDunsRedirectManualMatch";
    public static final String TRANSFORMER_NAME = "PrepareDunsRedirectManualMatchTransformer";

    private static final String EMP_SIZE_LARGE = PrepareDunsRedirectManualMatchConfig.LARGE_COMPANY_EMPLOYEE_SIZE;
    private static final String EMP_SIZE_SMALL = PrepareDunsRedirectManualMatchConfig.SMALL_COMPANY_EMPLOYEE_SIZE;

    @Inject
    private BeanValidationService beanValidationService;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        PrepareDunsRedirectManualMatchConfig config = getTransformerConfig(parameters);
        ValidationUtils.check(beanValidationService, config, PrepareDunsRedirectManualMatchConfig.class.getName());

        Node source = addSource(parameters.getBaseTables().get(0));

        // filter out invalid source rows and retain only required fields
        ManualSeedKeyPartitionRowFunction populateFn = new ManualSeedKeyPartitionRowFunction(
                new Fields(getSourceFieldNames(config)), config);
        FieldList srcFieldList = getSourceFieldList(config);
        source = preFilter(source, config).retain(srcFieldList);
        // generate rows for valid key partitions
        source = source.apply(populateFn, srcFieldList, source.getSchema(), srcFieldList, Fields.REPLACE);

        // add KeyPartition column
        String keyPartition = DunsRedirectBookConfig.KEY_PARTITION;
        AddFieldFunction addKeyPartitionFn = new AddFieldFunction(
                new AddKeyPartitionStrategy(keyPartition, getMatchKeyColumnNameMap(config)),
                new Fields(keyPartition));
        source = source.apply(
                addKeyPartitionFn, srcFieldList,
                Collections.singletonList(new FieldMetadata(keyPartition, String.class)),
                new FieldList(getSinkFieldNames(config)), Fields.ALL);

        return source;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PrepareDunsRedirectManualMatchConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    private Node preFilter(@NotNull Node source, @NotNull PrepareDunsRedirectManualMatchConfig config) {
        String employeeSize = config.getEmployeeSize();
        // CompanyName != null && (EmpSize == Large || EmpSize == Small)
        String exp = String.format("%s != null && (\"%s\".equals(%s) || \"%s\".equals(%s))",
                config.getCompanyName(),
                EMP_SIZE_LARGE, employeeSize,
                EMP_SIZE_SMALL, employeeSize);
        return source.filter(
                exp, new FieldList(config.getCompanyName(), config.getEmployeeSize()));
    }

    private FieldList getSourceFieldList(@NotNull PrepareDunsRedirectManualMatchConfig config) {
        return new FieldList(getSourceFieldNames(config));
    }

    /*
     * required source columns
     */
    private String[] getSourceFieldNames(@NotNull PrepareDunsRedirectManualMatchConfig config) {
        return new String[] {
                config.getManId(), config.getDuns(), config.getCompanyName(),
                config.getCountry(), config.getState(), config.getCity(),
                config.getEmployeeSize(), config.getSalesInBillions(),
                config.getTotalEmployees()
        };
    }

    /*
     * required source columns with an additional KeyPartition column
     */
    private String[] getSinkFieldNames(@NotNull PrepareDunsRedirectManualMatchConfig config) {
        String[] srcFields = getSourceFieldNames(config);
        String[] sinkFields = new String[srcFields.length + 1];
        System.arraycopy(srcFields, 0, sinkFields, 0, srcFields.length);
        sinkFields[srcFields.length] = DunsRedirectBookConfig.KEY_PARTITION;
        return sinkFields;
    }

    private Map<MatchKey, String> getMatchKeyColumnNameMap(@NotNull PrepareDunsRedirectManualMatchConfig config) {
        Map<MatchKey, String> map = new HashMap<>();
        map.put(MatchKey.Name, config.getCompanyName());
        map.put(MatchKey.Country, config.getCountry());
        map.put(MatchKey.State, config.getState());
        map.put(MatchKey.City, config.getCity());
        return map;
    }
}
