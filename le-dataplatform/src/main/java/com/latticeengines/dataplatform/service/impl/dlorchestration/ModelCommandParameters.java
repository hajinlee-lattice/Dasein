package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ModelCommandParameters {

    private static final char COMMA = ',';
    // Hard-coded name of HDFS subfolder
    public static final String EVENT_METADATA = "EventMetadata";

    // Mandatory parameters
    public static final String KEY_COLS = "KeyCols";
    public static final String MODEL_NAME = "ModelName";
    public static final String MODEL_TARGETS = "ModelTargets";
    public static final String EXCLUDE_COLUMNS = "ExcludeColumns";
    public static final String DL_URL = "DataLoader_Instance";
    public static final String DL_TENANT = "DataLoader_TenantName";
    public static final String DL_QUERY = "DataLoader_Query";

    // Optional parameters
    public static final String NUM_SAMPLES = "NumSamples";
    public static final String DEPIVOTED_EVENT_TABLE = "DepivotedEventTable";
    public static final String ALGORITHM_PROPERTIES = "AlgorithmProperties";
    public static final String ALGORITHM_SCRIPT = "AlgorithmScript";
    public static final String DEBUG = "Debug";
    public static final String VALIDATE = "Validate";
    public static final String FEATURE_THRESHOLD = "features_threshold";

    private String depivotedEventTable = null;
    private String metadataTable = EVENT_METADATA;
    private List<String> keyCols = Collections.emptyList();
    private int numSamples = 1; // 1 is default
    private String modelName = null;
    private List<String> modelTargets = Collections.emptyList();
    private List<String> excludeColumns = Collections.emptyList();
    private String algorithmProperties = null;
    private String algorithmScript = null;
    private String dlUrl = null;
    private String dlTenant = null;
    private String dlQuery = null;
    private boolean debug = false;
    private boolean validate = true;

    public ModelCommandParameters(List<ModelCommandParameter> commandParameters) {
        super();
        for (ModelCommandParameter parameter : commandParameters) {
            switch (parameter.getKey()) {
            case ModelCommandParameters.DEPIVOTED_EVENT_TABLE:
                this.setDepivotedEventTable(parameter.getValue());
                break;
            case ModelCommandParameters.KEY_COLS:
                this.setKeyCols(splitCommaSeparatedStringToList(parameter.getValue()));
                break;
            case ModelCommandParameters.MODEL_NAME:
                this.setModelName(parameter.getValue());
                break;
            case ModelCommandParameters.MODEL_TARGETS:
                this.setModelTargets(splitCommaSeparatedStringToList(parameter.getValue()));
                break;
            case ModelCommandParameters.NUM_SAMPLES:
                this.setNumSamples(Integer.parseInt(parameter.getValue()));
                break;
            case ModelCommandParameters.EXCLUDE_COLUMNS:
                this.setExcludeColumns(splitCommaSeparatedStringToList(parameter.getValue()));
                break;
            case ModelCommandParameters.ALGORITHM_PROPERTIES:
                this.setAlgorithmProperties(parameter.getValue());
                break;
            case ModelCommandParameters.ALGORITHM_SCRIPT:
                this.setAlgorithmScript(parameter.getValue());
                break;
            case ModelCommandParameters.DL_TENANT:
                this.setDlTenant(parameter.getValue());
                break;
            case ModelCommandParameters.DL_URL:
                this.setDlUrl(parameter.getValue());
                break;
            case ModelCommandParameters.DEBUG:
                this.setDebug(Boolean.parseBoolean(parameter.getValue()));
                break;
            case ModelCommandParameters.VALIDATE:
                this.setValidate(Boolean.parseBoolean(parameter.getValue()));
                break;

            case ModelCommandParameters.DL_QUERY:
                this.setDlQuery(parameter.getValue());
            }
        }

        List<String> missingParameters = new ArrayList<>();
        if (this.getKeyCols().isEmpty()) {
            missingParameters.add(ModelCommandParameters.KEY_COLS);
        }
        if (Strings.isNullOrEmpty(this.getModelName())) {
            missingParameters.add(ModelCommandParameters.MODEL_NAME);
        }
        if (this.getModelTargets().isEmpty()) {
            missingParameters.add(ModelCommandParameters.MODEL_TARGETS);
        }
        if (this.getExcludeColumns().isEmpty()) {
            missingParameters.add(ModelCommandParameters.EXCLUDE_COLUMNS);
        }
        if (Strings.isNullOrEmpty(this.getDlTenant())) {
            missingParameters.add(ModelCommandParameters.DL_TENANT);
        }
        if (Strings.isNullOrEmpty(this.getDlUrl())) {
            missingParameters.add(ModelCommandParameters.DL_URL);
        }
        if (Strings.isNullOrEmpty(this.getDlQuery())) {
            missingParameters.add(ModelCommandParameters.DL_QUERY);
        }
        if (!missingParameters.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_16000, new String[] { missingParameters.toString() });
        }
    }

    @VisibleForTesting
    List<String> splitCommaSeparatedStringToList(String input) {
        if (Strings.isNullOrEmpty(input)) {
            return Collections.emptyList();
        } else {
            return Splitter.on(COMMA).trimResults().omitEmptyStrings().splitToList(input);
        }
    }

    public String getDlUrl() {
        return dlUrl;
    }

    @VisibleForTesting
    void setDlUrl(String dlUrl) {
        this.dlUrl = dlUrl;
    }

    public String getDlTenant() {
        return dlTenant;
    }

    private void setDlTenant(String dlTenant) {
        this.dlTenant = dlTenant;
    }

    public String getDepivotedEventTable() {
        return depivotedEventTable;
    }

    private void setDepivotedEventTable(String depivotedEventTable) {
        this.depivotedEventTable = depivotedEventTable;
    }

    public String getMetadataTable() {
        return metadataTable;
    }

    public void setMetadataTable(String metadataTable) {
        this.metadataTable = metadataTable;
        ;
    }

    public List<String> getKeyCols() {
        return keyCols;
    }

    private void setKeyCols(List<String> keyCols) {
        this.keyCols = keyCols;
    }

    public int getNumSamples() {
        return numSamples;
    }

    public void setNumSamples(int numSamples) {
        this.numSamples = numSamples;
    }

    public String getModelName() {
        return modelName;
    }

    private void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public String getEventColumnName() {
        String eventColumn = getModelTargets().get(0);

        String[] cols = eventColumn.split(":");

        if (cols.length == 2) {
            return cols[1].trim();
        } else {
            return cols[0].trim();
        }
    }

    public List<String> getModelTargets() {
        return modelTargets;
    }

    public void setModelTargets(List<String> modelTargets) {
        this.modelTargets = modelTargets;
    }

    public List<String> getExcludeColumns() {
        return excludeColumns;
    }

    private void setExcludeColumns(List<String> excludeColumns) {
        this.excludeColumns = excludeColumns;
    }

    public String getAlgorithmProperties() {
        return algorithmProperties;
    }

    private void setAlgorithmProperties(String algorithmProperties) {
        this.algorithmProperties = algorithmProperties;
    }

    public String getAlgorithmScript() {
        return algorithmScript;
    }

    private void setAlgorithmScript(String algorithmScript) {
        this.algorithmScript = algorithmScript;
    }

    public String getDlQuery() {
        return dlQuery;
    }

    public void setDlQuery(String dlQuery) {
        this.dlQuery = dlQuery;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public boolean isValidate() {
        return validate;
    }

    public void setValidate(boolean validate) {
        this.validate = validate;
    }
}
