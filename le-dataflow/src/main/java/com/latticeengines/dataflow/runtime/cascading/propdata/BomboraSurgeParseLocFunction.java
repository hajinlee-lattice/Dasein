package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class BomboraSurgeParseLocFunction extends BaseOperation implements Function {

    private static final Logger log = LoggerFactory.getLogger(BomboraSurgeParseLocFunction.class);

    private static final long serialVersionUID = 8140067280192604076L;

    private Map<String, Integer> namePositionMap;
    private Map<String, List<NameLocation>> metroLocMap;
    private String metroAreaField;
    private String domainOriginField;
    private int countryLoc;
    private int stateLoc;
    private int cityLoc;

    public BomboraSurgeParseLocFunction(Fields fieldDeclaration, Map<String, List<NameLocation>> metroLocMap,
            String metroAreaField, String domainOriginField,
            String countryField, String stateField, String cityField) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.metroLocMap = metroLocMap;
        this.metroAreaField = metroAreaField;
        this.domainOriginField = domainOriginField;
        this.countryLoc = this.namePositionMap.get(countryField);
        this.stateLoc = this.namePositionMap.get(stateField);
        this.cityLoc = this.namePositionMap.get(cityField);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String metroArea = (String) arguments.getObject(metroAreaField);
        metroArea = standardizeBomboraMetroArea(metroArea);
        String domainOrigin = (String) arguments.getObject(domainOriginField);
        if (metroArea == null) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            result.set(countryLoc, domainOrigin);
            functionCall.getOutputCollector().add(result);
            return;
        }
        List<NameLocation> locations = metroLocMap.get(metroArea);
        if (locations == null) {
            log.info(String.format(
                    "Cannot find metro area in AllUniqueMetroCodes.csv file: %s (standardized: %s). Extracting location now...",
                    (String) arguments.getObject(metroAreaField), metroArea));
            locations = extractLocFromBomboraMetro(metroArea, domainOrigin);
        }
        for (NameLocation loc : locations) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            result.set(countryLoc, loc.getCountry());
            result.set(stateLoc, loc.getState());
            result.set(cityLoc, loc.getCity());
            functionCall.getOutputCollector().add(result);
        }
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }
    
    private String standardizeBomboraMetroArea(String metroArea) {
        if (StringUtils.isBlank(metroArea)) {
            return null;
        }
        metroArea = metroArea.replaceAll("\\(.*\\)", "");
        metroArea = metroArea.replace("\t", " ").replace("\n", "").replace("\r", "").trim();
        return metroArea;
    }

    /**
     * Sample metro codes:
     * Tampa / St.Pete / Sarasota , FL     --- 3 cities share same state
     * Tallahassee, FL / Thomasville, GA
     * Askim
     */
    private List<NameLocation> extractLocFromBomboraMetro(String metroArea, String country) {
        List<NameLocation> locations = new ArrayList<>();
        String[] areaList = metroArea.split("/");
        String state = null;
        country = StringUtils.isNotBlank(country) ? country.trim() : null;
        for (int i = areaList.length - 1; i >= 0; i--) {
            String area = areaList[i];
            if (StringUtils.isBlank(area)) {
                continue;
            }
            String[] loc = area.split(",");
            if (loc.length == 0 || loc.length > 2) { // unrecognized format
                continue;
            }
            if (loc.length == 2) {
                state = loc[1];
                state = StringUtils.isNotBlank(state) ? state.trim() : null;
            }
            String city = loc[0];
            city = StringUtils.isNotBlank(city) ? city.trim() : null;
            NameLocation location = new NameLocation();
            location.setCountry(country);
            // Remove Chinese invalid province
            if (!(country != null && state != null && country.equalsIgnoreCase("China") && state.contains("CN-"))) {
                location.setState(state);
            }
            location.setCity(city);
            locations.add(location);
        }
        return locations;
    }
}
