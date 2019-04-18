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
import com.latticeengines.transform.v2_0_25.common.StringLengthFcn;
import com.latticeengines.transform.v2_0_25.common.TransformFunctionBase;
import com.latticeengines.transform.v2_0_25.common.TransformWithImputationFunctionBase;

public class AddNameAttributes implements RealTimeTransform {

    private static final long serialVersionUID = 3003900822252312418L;

    private static final Logger log = LoggerFactory.getLogger(AddNameAttributes.class);

    private static final Map<String, TransformFunctionBase> attributeNameFunctions = new HashMap<>();
    private static final Map<String, TransformWithTwoArgsFunctionBase> attributeFirstAndLastNameFunctions = new HashMap<>();

    private int maxStrLen;

    private Map<String, Object> imputationMap = new HashMap<>();

    private abstract class TransformWithTwoArgsFunctionBase extends TransformWithImputationFunctionBase {
        TransformWithTwoArgsFunctionBase(Object imputation) {
            super(imputation);
        }

        @Override
        public double execute(String s) {
            return -1.0;
        }

        public abstract double execute(String s1, String s2);
    }

    private class FirstNameLastNameLengthFcn extends TransformWithTwoArgsFunctionBase {

        private StringLengthFcn stringLengthFcn;

        FirstNameLastNameLengthFcn(Object imputation, int maxStringLen) {
            super(imputation);
            stringLengthFcn = new StringLengthFcn(imputation, maxStringLen);
        }

        @Override
        public double execute(String firstName, String lastName) {
            if (firstName == null || lastName == null) {
                return getImputation();
            }
            return stringLengthFcn.execute(firstName) + stringLengthFcn.execute(lastName);
        }
    }

    private class FirstNameLastNameSameFcn extends TransformWithTwoArgsFunctionBase {

        FirstNameLastNameSameFcn(Object imputation) {
            super(imputation);
        }

        @Override
        public double execute(String firstName, String lastName) {
            if (firstName == null || lastName == null) {
                return getImputation();
            }

            if (firstName.toLowerCase().trim().equals(lastName.toLowerCase().trim())) {
                return 1.0;
            }
            return 0.0;
        }
    }

    private void initializeAttributeFunctions() {
        attributeNameFunctions.put("DS_FirstName_IsNull", new IsNullFcn());
        attributeNameFunctions.put("DS_LastName_IsNull", new IsNullFcn());

        if (imputationMap.containsKey("DS_FirstName_Length")) {
            attributeNameFunctions.put("DS_FirstName_Length",
                    new StringLengthFcn(imputationMap.get("DS_FirstName_Length"), maxStrLen));
        }

        if (imputationMap.containsKey("DS_LastName_Length")) {
            attributeNameFunctions.put("DS_LastName_Length",
                    new StringLengthFcn(imputationMap.get("DS_LastName_Length"), maxStrLen));
        }

        if (imputationMap.containsKey("DS_Name_Length")) {
            attributeFirstAndLastNameFunctions.put("DS_Name_Length",
                    new FirstNameLastNameLengthFcn(imputationMap.get("DS_Name_Length"), maxStrLen));
        }

        if (imputationMap.containsKey("DS_FirstName_SameAsLastName")) {
            attributeFirstAndLastNameFunctions.put("DS_FirstName_SameAsLastName",
                    new FirstNameLastNameSameFcn(imputationMap.get("DS_FirstName_SameAsLastName")));
        }
    }

    AddNameAttributes(String modelPath) {
        importLoookupMapFromJson(modelPath + "/dsnameimputations.json");
        initializeAttributeFunctions();
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {

        if (arguments.get("column3") != null) {

            String firstNameColumn = (String) arguments.get("column1");
            String lastNameColumn = (String) arguments.get("column2");
            String nameTrfColumn = (String) arguments.get("column3");
            String firstName = (String) record.get(firstNameColumn);
            String lastName = (String) record.get(lastNameColumn);

            if (attributeFirstAndLastNameFunctions.containsKey(nameTrfColumn)) {
                return attributeFirstAndLastNameFunctions.get(nameTrfColumn).execute(firstName, lastName);
            } else {
                return null;
            }
        }

        String nameColumn = (String) arguments.get("column1");
        String name = (String) record.get(nameColumn);

        String nameTrfColumn = (String) arguments.get("column2");

        if (attributeNameFunctions.containsKey(nameTrfColumn)) {
            return attributeNameFunctions.get(nameTrfColumn).execute(name);
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
            Map<String, Object> nameAttributeData = JsonUtils.deserialize(contents, Map.class, true);
            for (Map.Entry<String, Object> entry : nameAttributeData.entrySet()) {
                if (entry.getKey().equals("maxStrLen")) {
                    maxStrLen = (int) entry.getValue();
                    log.info(String.format("Loaded maxStrLen = %d", maxStrLen));
                } else {
                    imputationMap.put(entry.getKey(), entry.getValue());
                    log.info(String.format("Loaded Imputation for %s", entry.getKey()));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Cannot open json file with title imputation values: %s", filename), e);
        }
    }
}
