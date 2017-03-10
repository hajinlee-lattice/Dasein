package com.latticeengines.transform.v2_0_25.functions;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;
import com.latticeengines.transform.v2_0_25.common.IsInvalidFcn;
import com.latticeengines.transform.v2_0_25.common.IsNullFcn;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;
import com.latticeengines.transform.v2_0_25.common.StringEntropyFcn;
import com.latticeengines.transform.v2_0_25.common.StringLengthFcn;
import com.latticeengines.transform.v2_0_25.common.TransformFunctionBase;

public class AddCompanyNameAttributes implements RealTimeTransform {

    private static final long serialVersionUID = 2330775612672315040L;

    private static final Log log = LogFactory.getLog(AddCompanyNameAttributes.class);

    private static final Map<String, TransformFunctionBase> attributeFunctions = new HashMap<>();

    private int maxStrLen;
    private String unusualCharSet = "";
    String vetoStringSet = "";

    private Map<String, Object> imputationMap = new HashMap<>();

    private void initializeAttributeFunctions() {
        attributeFunctions.put("DS_Company_IsNull", new IsNullFcn());

        if (imputationMap.containsKey("DS_Company_Length")) {
            attributeFunctions.put("DS_Company_Length",
                    new StringLengthFcn(imputationMap.get("DS_Company_Length"), maxStrLen));
        }

        if (imputationMap.containsKey("DS_Company_NameHasUnusualChar")) {
            attributeFunctions.put("DS_Company_NameHasUnusualChar", new IsInvalidFcn(
                    imputationMap.get("DS_Company_NameHasUnusualChar"), unusualCharSet, vetoStringSet));
        }

        if (imputationMap.containsKey("DS_Company_Entropy")) {
            attributeFunctions.put("DS_Company_Entropy",
                    new StringEntropyFcn(imputationMap.get("DS_Company_Entropy"), maxStrLen));
        }
    }

    public AddCompanyNameAttributes(String modelPath) {
        importLoookupMapFromJson(modelPath + "/dscompanyimputations.json");
        initializeAttributeFunctions();
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {

        String nameColumn = (String) arguments.get("column1");
        String name = (String) record.get(nameColumn);

        String nameTrfColumn = (String) arguments.get("column2");

        if (attributeFunctions.containsKey(nameTrfColumn)) {
            return attributeFunctions.get(nameTrfColumn).execute(name);
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
            String contents = FileUtils.readFileToString(new File(filename));
            Map<String, Object> titleAttributeData = JsonUtils.deserialize(contents, Map.class, true);
            for (Map.Entry<String, Object> entry : titleAttributeData.entrySet()) {
                if (entry.getKey().equals("maxStrLen")) {
                    maxStrLen = (int) entry.getValue();
                    log.info(String.format("Loaded maxStrLen = %d", maxStrLen));
                } else if (entry.getKey().equals("unusualCharacterSet")) {
                    unusualCharSet = (String) entry.getValue();
                    log.info(String.format("Loaded unusualCharSet = %s", unusualCharSet));
                } else if (entry.getKey().equals("badSet")) {
                    vetoStringSet = (String) entry.getValue();
                    log.info(String.format("Loaded vetoStringSet = %s", vetoStringSet));
                } else {
                    imputationMap.put(entry.getKey(), entry.getValue());
                    log.info(String.format("Loaded Imputation for %s", entry.getKey()));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Cannot open json file with CompanyName imputation values: %s", filename), e);
        }
    }
}