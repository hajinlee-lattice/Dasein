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

public class AddEmailAttributes implements RealTimeTransform {

    private static final long serialVersionUID = 5210821054675195298L;

    private static final Logger log = LoggerFactory.getLogger(AddEmailAttributes.class);

    private static final Map<String, TransformFunctionBase> attributeFunctions = new HashMap<>();

    private int maxStrLen;
    private int invalidEmailThld;

    private Map<String, Object> imputationMap = new HashMap<>();

    private class EmailIsInvalidFcn extends TransformWithImputationFunctionBase {

        private int maxStringLen;

        EmailIsInvalidFcn(Object imputation, int maxStringLen) {
            super(imputation);
            this.maxStringLen = maxStringLen;
        }

        @Override
        public void setImputation(Object imputation) {
            // imputation may contain either a Double or an Integer
            super.setImputation(((Number) imputation).doubleValue());
        }

        @Override
        public double execute(String s) {
            if (s == null) {
                return getImputation();
            }
            if (s.length() < maxStringLen || !s.contains("@")) {
                return 1.0;
            }
            return 0.0;
        }

    }

    private void initializeAttributeFunctions() {
        attributeFunctions.put("DS_Email_IsNull", new IsNullFcn());

        if (imputationMap.containsKey("DS_Email_Length")) {
            attributeFunctions.put("DS_Email_Length",
                    new StringLengthFcn(imputationMap.get("DS_Email_Length"), maxStrLen));
        }

        if (imputationMap.containsKey("DS_Email_IsInvalid")) {
            attributeFunctions.put("DS_Email_IsInvalid",
                    new EmailIsInvalidFcn(imputationMap.get("DS_Email_IsInvalid"), invalidEmailThld));
        }

        if (imputationMap.containsKey("DS_Email_PrefixLength")) {
            attributeFunctions.put("DS_Email_PrefixLength",
                    new StringLengthFcn(imputationMap.get("DS_Email_PrefixLength"), maxStrLen));
        }

        if (imputationMap.containsKey("DS_Domain_Length")) {
            attributeFunctions.put("DS_Domain_Length",
                    new StringLengthFcn(imputationMap.get("DS_Domain_Length"), maxStrLen));
        }
    }

    public AddEmailAttributes(String modelPath) {
        importLoookupMapFromJson(modelPath + "/dsemailimputations.json");
        initializeAttributeFunctions();
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {

        String emailColumn = (String) arguments.get("column1");
        String email = (String) record.get(emailColumn);

        String emailTrfColumn = (String) arguments.get("column2");

        if (attributeFunctions.containsKey(emailTrfColumn)) {
            if (emailTrfColumn.equals("DS_Email_PrefixLength")) {
                String prefix = null;
                if (email != null) {
                    int index = email.indexOf('@');
                    if (index < 1)
                        return 0.0;
                    prefix = email.substring(0, index);
                }
                return attributeFunctions.get(emailTrfColumn).execute(prefix);
            } else if (emailTrfColumn.equals("DS_Domain_Length")) {
                String domain = null;
                if (email != null) {
                    int index = email.indexOf('@');
                    if (index < 0)
                        return 0.0;
                    if (index + 1 > email.length())
                        return -1.0;
                    domain = email.substring(index + 1);
                }
                return attributeFunctions.get(emailTrfColumn).execute(domain);
            }
            return attributeFunctions.get(emailTrfColumn).execute(email);
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
                if (entry.getKey().equals("maxStrLen")) {
                    maxStrLen = (int) entry.getValue();
                    log.info(String.format("Loaded maxStrLen = %d", maxStrLen));
                } else if (entry.getKey().equals("invalidEmailThld")) {
                    invalidEmailThld = (int) entry.getValue();
                    log.info(String.format("Loaded invalidEmailThld = %d", invalidEmailThld));
                } else {
                    imputationMap.put(entry.getKey(), entry.getValue());
                    log.info(String.format("Loaded Imputation for %s", entry.getKey()));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Cannot open json file with Email imputation values: %s", filename), e);
        }
    }
}
