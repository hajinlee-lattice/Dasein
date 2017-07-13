package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.LocationUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes"})
public class CountryStandardizationFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -2044488912062585735L;

    private static final Logger log = LoggerFactory.getLogger(CountryStandardizationFunction.class);

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
        country = LocationUtils.getStandardCountry(country);
        if (standardCountries.containsKey(country)) {
            country = standardCountries.get(country);
        } else {
            log.warn(String.format("Fail to standardize country %s", country));
        }
        functionCall.getOutputCollector().add(new Tuple(country));
    }
}
