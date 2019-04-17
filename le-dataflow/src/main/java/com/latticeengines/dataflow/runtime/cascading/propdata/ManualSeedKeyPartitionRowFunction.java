package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PrepareDunsRedirectManualMatchConfig;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Generate one or multiple intermediate rows for each valid key partition that
 * contains the required information to populate DunsRedirectBook.
 */
@SuppressWarnings("rawtypes")
public class ManualSeedKeyPartitionRowFunction extends BaseOperation implements Function {
    private static final long serialVersionUID = -4314247333801902743L;
    private static final String EMP_SIZE_SMALL = PrepareDunsRedirectManualMatchConfig.SMALL_COMPANY_EMPLOYEE_SIZE;

    private final String nameField;
    private final String countryField;
    private final String stateField;
    private final String cityField;
    private final String empSizeField;
    private final int countryFieldIdx;
    private final int stateFieldIdx;
    private final int cityFieldIdx;

    public ManualSeedKeyPartitionRowFunction(@NotNull Fields fieldDeclaration,
            @NotNull PrepareDunsRedirectManualMatchConfig config) {
        super(fieldDeclaration);
        // fieldName => idx in Fields
        Map<String, Integer> idxMap = getIndexMap();
        this.nameField = config.getCompanyName();
        this.countryField = config.getCountry();
        this.stateField = config.getState();
        this.cityField = config.getCity();
        this.empSizeField = config.getEmployeeSize();
        this.countryFieldIdx = idxMap.get(countryField);
        this.stateFieldIdx = idxMap.get(stateField);
        this.cityFieldIdx = idxMap.get(cityField);
    }

    /*
     * arguments will contain valid employee size field
     */
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String employeeSize = arguments.getString(empSizeField);
        String name = arguments.getString(nameField);
        String country = arguments.getString(countryField);
        String state = arguments.getString(stateField);
        String city = arguments.getString(cityField);
        if (StringUtils.isNotBlank(name)) {
            functionCall.getOutputCollector().add(getResult(arguments, null, null, null));
        }
        if (StringUtils.isNotBlank(name) && StringUtils.isNotBlank(country)) {
            functionCall.getOutputCollector().add(getResult(arguments, country, null, null));
        }
        // name, country, state
        if (StringUtils.isNotBlank(name) && StringUtils.isNotBlank(country)
                && StringUtils.isNotBlank(state)) {
            functionCall.getOutputCollector().add(getResult(arguments, country, state, null));
        }
        // only for small company
        if (EMP_SIZE_SMALL.equals(employeeSize)) {
            // name, country, city
            if (StringUtils.isNotBlank(name) && StringUtils.isNotBlank(country)
                    && StringUtils.isNotBlank(city)) {
                functionCall.getOutputCollector().add(getResult(arguments, country, null, city));
            }
            // name, country, state, city
            if (StringUtils.isNotBlank(name) && StringUtils.isNotBlank(country)
                    && StringUtils.isNotBlank(state) && StringUtils.isNotBlank(city)) {
                functionCall.getOutputCollector().add(getResult(arguments, country, state, city));
            }
        }
    }

    private Tuple getResult(TupleEntry arguments, String country, String state, String city) {
        Tuple result = getResult(arguments);
        // clear location fields
        if (country == null) {
            result.set(countryFieldIdx, null);
        }
        if (state == null) {
            result.set(stateFieldIdx, null);
        }
        if (city == null) {
            result.set(cityFieldIdx, null);
        }
        return result;
    }

    private Tuple getResult(TupleEntry arguments) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        for (int i = 0; i < arguments.size(); i++) {
            result.set(i, arguments.getObject(i));
        }
        return result;
    }

    private Map<String, Integer> getIndexMap() {
        final Fields fields = getFieldDeclaration();
        // setup map from fieldName to index in declaration array
        // because set by index is more efficient than by fieldName
        return IntStream.range(0, fields.size())
                .mapToObj(idx -> Pair.of(idx, (String) fields.get(idx)))
                .collect(Collectors.toMap(Pair::getValue, Pair::getKey));
    }
}
