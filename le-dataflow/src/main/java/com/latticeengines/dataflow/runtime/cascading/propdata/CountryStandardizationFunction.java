package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Map;

import com.latticeengines.common.exposed.util.LocationUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class CountryStandardizationFunction extends BaseOperation implements Function {
    private Map<String, String> standardCountries;
    private String countryField;

    public CountryStandardizationFunction(String countryField, Map<String, String> standardCountries) {
        super(new Fields(countryField));
        this.countryField = countryField;
        this.standardCountries = standardCountries;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String country = arguments.getString(countryField);
        country = LocationUtils.getStandardCountryDefaultNull(country);
        if (standardCountries.containsKey(country)) {
            country = standardCountries.get(country);
        }
        functionCall.getOutputCollector().add(new Tuple(country));
    }
}
