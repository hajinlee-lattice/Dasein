package com.latticeengines.transform.v2_0_25.functions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;
import com.latticeengines.transform.v2_0_25.common.CategoryRegexMatchFcn;
import com.latticeengines.transform.v2_0_25.common.IsInvalidFcn;
import com.latticeengines.transform.v2_0_25.common.IsNullFcn;
import com.latticeengines.transform.v2_0_25.common.IsRegexMatchFcn;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;
import com.latticeengines.transform.v2_0_25.common.StringLengthFcn;
import com.latticeengines.transform.v2_0_25.common.TitleLevelFcn;
import com.latticeengines.transform.v2_0_25.common.TransformFunctionBase;

public class AddTitleAttributesV2 implements RealTimeTransform {

    private static final long serialVersionUID = -653758845266475629L;

    private static final Logger log = LoggerFactory.getLogger(AddTitleAttributesV2.class);

    private static final Map<String, TransformFunctionBase> attributeFunctions = new HashMap<>();

    private int maxTitleLen;
    private String unusualCharSet = "";
    private String vetoStringSet = "";
    private String regexAcademic = "";
    private String regexTech = "";
    private String regexDirector = "";
    private String regexSenior = "";
    private String regexVP = "";
    private String regexManager = "";
    private List<List<String>> regexTitleChannelMap = new ArrayList<>();
    private List<List<String>> regexTitleFunctionMap = new ArrayList<>();
    private List<List<String>> regexTitleRoleMap = new ArrayList<>();
    private List<List<String>> regexTitleScopeMap = new ArrayList<>();

    private Map<String, Object> imputationMap = new HashMap<>();

    private void initializeAttributeFunctions() {
        attributeFunctions.put("DS_Title_IsNull", new IsNullFcn());

        if (imputationMap.containsKey("DS_Title_Length")) {
            attributeFunctions.put("DS_Title_Length",
                    new StringLengthFcn(imputationMap.get("DS_Title_Length"), maxTitleLen));
        }

        if (imputationMap.containsKey("DS_Title_IsAcademic")) {
            attributeFunctions.put("DS_Title_IsAcademic",
                    new IsRegexMatchFcn(imputationMap.get("DS_Title_IsAcademic"), regexAcademic));
        }

        if (imputationMap.containsKey("DS_Title_IsTechRelated")) {
            attributeFunctions.put("DS_Title_IsTechRelated",
                    new IsRegexMatchFcn(imputationMap.get("DS_Title_IsTechRelated"), regexTech));
        }

        if (imputationMap.containsKey("DS_Title_IsDirector")) {
            attributeFunctions.put("DS_Title_IsDirector",
                    new IsRegexMatchFcn(imputationMap.get("DS_Title_IsDirector"), regexDirector));
        }

        if (imputationMap.containsKey("DS_Title_IsSenior")) {
            attributeFunctions.put("DS_Title_IsSenior",
                    new IsRegexMatchFcn(imputationMap.get("DS_Title_IsSenior"), regexSenior));
        }

        if (imputationMap.containsKey("DS_Title_IsVPAbove")) {
            attributeFunctions.put("DS_Title_IsVPAbove",
                    new IsRegexMatchFcn(imputationMap.get("DS_Title_IsVPAbove"), regexVP));
        }

        if (imputationMap.containsKey("DS_Title_IsManager")) {
            attributeFunctions.put("DS_Title_IsManager",
                    new IsRegexMatchFcn(imputationMap.get("DS_Title_IsManager"), regexManager));
        }

        if (imputationMap.containsKey("DS_Title_Channel")) {
            attributeFunctions.put("DS_Title_Channel", new CategoryRegexMatchFcn(imputationMap.get("DS_Title_Channel"),
                    unusualCharSet, vetoStringSet, regexTitleChannelMap));
        }

        if (imputationMap.containsKey("DS_Title_Function")) {
            attributeFunctions.put("DS_Title_Function", new CategoryRegexMatchFcn(
                    imputationMap.get("DS_Title_Function"), unusualCharSet, vetoStringSet, regexTitleFunctionMap));
        }

        if (imputationMap.containsKey("DS_Title_Role")) {
            attributeFunctions.put("DS_Title_Role", new CategoryRegexMatchFcn(imputationMap.get("DS_Title_Role"),
                    unusualCharSet, vetoStringSet, regexTitleRoleMap));
        }

        if (imputationMap.containsKey("DS_Title_Scope")) {
            attributeFunctions.put("DS_Title_Scope", new CategoryRegexMatchFcn(imputationMap.get("DS_Title_Scope"),
                    unusualCharSet, vetoStringSet, regexTitleScopeMap));
        }

        if (imputationMap.containsKey("DS_Title_HasUnusualChar")) {
            attributeFunctions.put("DS_Title_HasUnusualChar",
                    new IsInvalidFcn(imputationMap.get("DS_Title_HasUnusualChar"), unusualCharSet, vetoStringSet));
        }

        if (imputationMap.containsKey("DS_Title_Level")) {
            attributeFunctions.put("DS_Title_Level", new TitleLevelFcn(imputationMap.get("DS_Title_Level"), regexSenior,
                    regexManager, regexDirector, regexVP));
        }

        if (imputationMap.containsKey("DS_Title_Level_Categorical")) {
            List<List<String>> regexTitleLevelMap = new ArrayList<>();
            regexTitleLevelMap.add(Arrays.asList("VP", regexVP));
            regexTitleLevelMap.add(Arrays.asList("Director", regexDirector));
            regexTitleLevelMap.add(Arrays.asList("Manager", regexManager));
            attributeFunctions.put("DS_Title_Level_Categorical",
                    new CategoryRegexMatchFcn(imputationMap.get("DS_Title_Level_Categorical"), unusualCharSet,
                            vetoStringSet, regexTitleLevelMap));
        }

    }

    public AddTitleAttributesV2(String modelPath) {
        importLoookupMapFromJson(modelPath + "/dstitleimputations.json");
        initializeAttributeFunctions();
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {

        String titleColumn = (String) arguments.get("column1");
        String title = (String) record.get(titleColumn);

        String titleTrfColumn = (String) arguments.get("column2");

        if (attributeFunctions.containsKey(titleTrfColumn)) {
            return attributeFunctions.get(titleTrfColumn).execute(title);
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
                if (entry.getKey().equals("maxTitleLen")) {
                    maxTitleLen = (int) entry.getValue();
                    log.info(String.format("Loaded maxTitleLen = %d", maxTitleLen));
                } else if (entry.getKey().equals("unusualCharacterSet")) {
                    unusualCharSet = (String) entry.getValue();
                    log.info(String.format("Loaded unusualCharSet = %s", unusualCharSet));
                } else if (entry.getKey().equals("badSet")) {
                    vetoStringSet = (String) entry.getValue();
                    log.info(String.format("Loaded vetoStringSet = %s", vetoStringSet));
                } else if (entry.getKey().equals("acadStr")) {
                    regexAcademic = (String) entry.getValue();
                    log.info(String.format("Loaded regexAcademic = %s", regexAcademic));
                } else if (entry.getKey().equals("techStr")) {
                    regexTech = (String) entry.getValue();
                    log.info(String.format("Loaded regexTech = %s", regexTech));
                } else if (entry.getKey().equals("dirStr")) {
                    regexDirector = (String) entry.getValue();
                    log.info(String.format("Loaded regexDirector = %s", regexDirector));
                } else if (entry.getKey().equals("senrStr")) {
                    regexSenior = (String) entry.getValue();
                    log.info(String.format("Loaded regexSenior = %s", regexSenior));
                } else if (entry.getKey().equals("vpStr")) {
                    regexVP = (String) entry.getValue();
                    log.info(String.format("Loaded regexVP = %s", regexVP));
                } else if (entry.getKey().equals("mgrStr")) {
                    regexManager = (String) entry.getValue();
                    log.info(String.format("Loaded regexManager = %s", regexManager));
                } else if (entry.getKey().equals("mapTitleChannelAny")) {
                    regexTitleChannelMap = (List<List<String>>) entry.getValue();
                    log.info("Loaded regexTitleChannelMap");
                } else if (entry.getKey().equals("mapTitleFunctionAny")) {
                    regexTitleFunctionMap = (List<List<String>>) entry.getValue();
                    log.info("Loaded regexTitleFunctionMap");
                } else if (entry.getKey().equals("mapTitleRoleAny")) {
                    regexTitleRoleMap = (List<List<String>>) entry.getValue();
                    log.info("Loaded regexTitleRoleMap");
                } else if (entry.getKey().equals("mapTitleScopeAny")) {
                    regexTitleScopeMap = (List<List<String>>) entry.getValue();
                    log.info("Loaded regexTitleScopeMap");
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
