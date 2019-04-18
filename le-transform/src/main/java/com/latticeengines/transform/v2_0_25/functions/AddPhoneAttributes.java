package com.latticeengines.transform.v2_0_25.functions;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;
import com.latticeengines.transform.v2_0_25.common.IsNullFcn;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;
import com.latticeengines.transform.v2_0_25.common.TransformFunctionBase;
import com.latticeengines.transform.v2_0_25.common.TransformWithImputationFunctionBase;

public class AddPhoneAttributes implements RealTimeTransform {

    private static final long serialVersionUID = 1043623053362231011L;

    private static final Logger log = LoggerFactory.getLogger(AddPhoneAttributes.class);

    private static final Map<String, TransformFunctionBase> attributeFunctions = new HashMap<>();

    private Map<String, Object> imputationMap = new HashMap<>();

    private class PhoneEntropyFcn extends TransformWithImputationFunctionBase {

        PhoneEntropyFcn(Object imputation) {
            super(imputation);
        }

        @Override
        public double execute(String s) {
            if (s == null) {
                return getImputation();
            }

            Map<Character, Integer> occurences = new HashMap<Character, Integer>();

            int lengthAsInt = 0;
            for (int i = 0; i < s.length(); i++) {
                if (!Character.isDigit(s.charAt(i))) {
                    continue;
                }
                if (occurences.containsKey(s.charAt(i))) {
                    int oldValue = occurences.get(s.charAt(i));
                    occurences.put(s.charAt(i), oldValue + 1);
                } else {
                    occurences.put(s.charAt(i), 1);
                }
                lengthAsInt++;
            }

            double entropy = 0.0;
            final double length = lengthAsInt;
            for (Map.Entry<Character, Integer> entry : occurences.entrySet()) {
                final double prob = entry.getValue().doubleValue() / length;
                entropy -= prob * Math.log(prob);
            }
            return entropy;
        }

    }

    private void initializeAttributeFunctions() {
        attributeFunctions.put("DS_Phone_IsNull", new IsNullFcn());

        if (imputationMap.containsKey("DS_Phone_Entropy")) {
            attributeFunctions.put("DS_Phone_Entropy", new PhoneEntropyFcn(imputationMap.get("DS_Phone_Entropy")));
        }
    }

    public AddPhoneAttributes(String modelPath) {
        importLoookupMapFromJson(modelPath + "/dsphoneimputations.json");
        initializeAttributeFunctions();
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {

        String phoneColumn = (String) arguments.get("column1");
        String phone = (String) record.get(phoneColumn);

        String phoneTrfColumn = (String) arguments.get("column2");

        if (attributeFunctions.containsKey(phoneTrfColumn)) {
            return attributeFunctions.get(phoneTrfColumn).execute(phone);
        }

        return null;
    }

    @Override
    public TransformMetadata getMetadata() {
        return null;
    }

    public Map<String, Object> getTitleImputationMap() {
        return imputationMap;
    }

    @SuppressWarnings("unchecked")
    private void importLoookupMapFromJson(String filename) {
        try {
            @SuppressWarnings("deprecation")
            String contents = FileUtils.readFileToString(new File(filename));
            Map<String, Object> attributeData = JsonUtils.deserialize(contents, Map.class, true);
            for (Map.Entry<String, Object> entry : attributeData.entrySet()) {
                imputationMap.put(entry.getKey(), entry.getValue());
                log.info(String.format("Loaded Imputation for %s", entry.getKey()));
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Cannot open json file with Email imputation values: %s", filename), e);
        }
    }
}
