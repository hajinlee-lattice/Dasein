package com.latticeengines.transform.v2_0_25.functions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class AddTitleAttributes implements RealTimeTransform {

    private static final long serialVersionUID = 507637584133936112L;

    private static final Logger log = LoggerFactory.getLogger(AddTitleAttributes.class);

    protected int maxTitleLen;
    protected List<String> missingValues = new ArrayList<>();
    protected Map<String, Double> imputationMap = new HashMap<>();

    public AddTitleAttributes() {
    }

    public AddTitleAttributes(String modelPath) {
        importLoookupMapFromJson(modelPath + "/dstitleimputations.json");
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {

        String titleColumn = (String) arguments.get("column1");
        String title = (String) record.get(titleColumn);

        String titleLengthColumn = (String) arguments.get("column2");

        if (title == null || missingValues.contains(title))
            return imputationMap.get(titleLengthColumn);

        return new Double(Math.min(title.length(), maxTitleLen));
    }

    @Override
    public TransformMetadata getMetadata() {
        return null;
    }

    public Map<String, Double> getTitleImputationMap() {
        return imputationMap;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void importLoookupMapFromJson(String filename) {
        try {
            String contents = FileUtils.readFileToString(new File(filename));
            Map<String, Object> titleAttributeData = JsonUtils.deserialize(contents, Map.class, true);
            for (Map.Entry<String, Object> entry : titleAttributeData.entrySet()) {
                if (entry.getKey().equals("maxTitleLen")) {
                    maxTitleLen = (int) entry.getValue();
                    log.info(String.format("Loaded maxTitleLen = %d", maxTitleLen));
                } else if (entry.getKey().equals("missingValues")) {
                    missingValues = (List) entry.getValue();
                    for (String value : missingValues) {
                        log.info(String.format("Loaded Missing Value: %s", value));
                    }
                } else {
                    imputationMap.put(entry.getKey(), (Double) entry.getValue());
                    log.info(String.format("Loaded Imputation Value %f for %s", entry.getValue(), entry.getKey()));
                }
            }
        } catch (FileNotFoundException e) {
            log.warn(String.format("Cannot find json file with title imputation values: %s", filename));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Cannot open json file with title imputation values: %s", filename), e);
        }
    }

}
