package com.latticeengines.dataplatform.service.impl;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Joiner;
import com.latticeengines.dataplatform.service.impl.dlorchestration.ModelCommandParameters;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandId;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;

public class ModelingServiceTestUtils {

    public static final int NUM_SAMPLES = 1;

    public static final String ALGORITHM_PROPERTIES_FEATURES_THRESHOLD = "criterion=gini n_estimators=10 n_jobs=4 min_samples_split=25 min_samples_leaf=10 max_depth=8 bootstrap=True features_threshold=";

    public static final String EVENT_TABLE = "Q_EventTable_Nutanix";

    public static List<String> createExcludeList() {
        List<String> excludeList = new ArrayList<>();
        excludeList.add("Nutanix_EventTable_Clean");
        excludeList.add("P1_Event");
        excludeList.add("P1_Target");
        excludeList.add("P1_TargetTraining");
        excludeList.add("PeriodID");
        excludeList.add("CustomerID");
        excludeList.add("AwardYear");
        excludeList.add("FundingFiscalQuarter");
        excludeList.add("FundingFiscalYear");
        excludeList.add("BusinessAssets");
        excludeList.add("BusinessEntityType");
        excludeList.add("BusinessIndustrySector");
        excludeList.add("RetirementAssetsYOY");
        excludeList.add("RetirementAssetsEOY");
        excludeList.add("TotalParticipantsSOY");
        excludeList.add("BusinessType");
        excludeList.add("LeadID");
        excludeList.add("Company");
        excludeList.add("Domain");
        excludeList.add("Email");
        excludeList.add("LeadSource");

        return excludeList;
    }

    public static ModelCommand createModelCommandWithCommandParameters(long pid) {
        return createModelCommandWithCommandParameters(pid, EVENT_TABLE);
    }

    public static ModelCommand createModelCommandWithCommandParameters(long pid, int featuresThreshold) {
        List<ModelCommandParameter> parameters = new ArrayList<>();
        ModelCommand command = new ModelCommand(pid, "Nutanix", "Nutanix", ModelCommandStatus.NEW, parameters,
                ModelCommand.TAHOE, EVENT_TABLE);
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DEPIVOTED_EVENT_TABLE,
                "Q_EventTableDepivot_Nutanix"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.KEY_COLS, "Nutanix_EventTable_Clean"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.MODEL_NAME, "Model_Submission1"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.MODEL_TARGETS, "P1_Event"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.NUM_SAMPLES, String
                .valueOf(NUM_SAMPLES)));
        String excludeString = Joiner.on(",").join(ModelingServiceTestUtils.createExcludeList());
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.EXCLUDE_COLUMNS, excludeString));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_TENANT, "VisiDBTest"));
        try {
            parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_URL, "http://"
                    + getPublicIpAddress() + ":8082/DLRestService"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_QUERY, "Q_DataForModeling"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DEBUG, "true"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.VALIDATE, "false"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.ALGORITHM_PROPERTIES, String
                .valueOf(ModelingServiceTestUtils.buildAlgorithmPropertiesStringWithFeatureThreshold(featuresThreshold))));
        return command;
    }

    public static ModelCommand createModelCommandWithCommandParameters(long pid, String eventTable) {
        return createModelCommandWithCommandParameters(pid, eventTable, false, true);
    }

    public static ModelCommand createModelCommandWithCommandParameters(long pid, String eventTable, boolean debug,
            boolean validate) {
        List<ModelCommandParameter> parameters = new ArrayList<>();

        ModelCommand command = new ModelCommand(pid, "Nutanix", "Nutanix", ModelCommandStatus.NEW, parameters,
                ModelCommand.TAHOE, eventTable);
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DEPIVOTED_EVENT_TABLE,
                "Q_EventTableDepivot_Nutanix"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.KEY_COLS, "Nutanix_EventTable_Clean"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.MODEL_NAME, "Model_Submission1"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.MODEL_TARGETS, "P1_Event"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.NUM_SAMPLES, String
                .valueOf(NUM_SAMPLES)));
        String excludeString = Joiner.on(",").join(ModelingServiceTestUtils.createExcludeList());
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.EXCLUDE_COLUMNS, excludeString));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_TENANT, "VisiDBTest"));
        try {
            parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_URL, "http://"
                    + getPublicIpAddress() + ":8082/DLRestService"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_QUERY, "Q_DataForModeling"));
        if (debug) {
            parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DEBUG, "true"));
        }
        if (!validate) {
            parameters.add(new ModelCommandParameter(command, ModelCommandParameters.VALIDATE, "false"));
        }
        return command;
    }

    public static ModelCommand createModelCommandWithCommandParametersFeatureSelection(long pid, String eventTable, boolean debug,
            boolean validate) {
        List<ModelCommandParameter> parameters = new ArrayList<>();

        ModelCommand command = new ModelCommand(pid, "Nutanix", "Nutanix", ModelCommandStatus.NEW, parameters,
                ModelCommand.TAHOE, eventTable);
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DEPIVOTED_EVENT_TABLE,
                "Q_EventTableDepivot_Nutanix"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.KEY_COLS, "Nutanix_EventTable_Clean"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.MODEL_NAME, "Model_Submission1"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.MODEL_TARGETS, "P1_Event"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.NUM_SAMPLES, String
                .valueOf(NUM_SAMPLES)));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.ALGORITHM_PROPERTIES, String
                .valueOf(ModelingServiceTestUtils.buildAlgorithmPropertiesStringWithFeatureThreshold(30))));
        String excludeString = Joiner.on(",").join(ModelingServiceTestUtils.createExcludeList());
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.EXCLUDE_COLUMNS, excludeString));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_TENANT, "VisiDBTest"));
        try {
            parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_URL, "http://"
                    + getPublicIpAddress() + ":8082/DLRestService"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_QUERY, "Q_DataForModeling"));
        if (debug) {
            parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DEBUG, "true"));
        }
        if (!validate) {
            parameters.add(new ModelCommandParameter(command, ModelCommandParameters.VALIDATE, "false"));
        }
        return command;
    }

    public static ModelCommand createModelCommandWithFewRowsAndReadoutTargets(long pid, String eventTable,
            boolean debug, boolean validate) {
        List<ModelCommandParameter> parameters = new ArrayList<>();
        ModelCommand command = new ModelCommand(pid, "FewRowsNutanix", "FewRowsNutanix", ModelCommandStatus.NEW,
                parameters, ModelCommand.TAHOE, eventTable);
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DEPIVOTED_EVENT_TABLE,
                "Q_EventTableDepivot_Nutanix_FewRows"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.KEY_COLS, "Nutanix_EventTable_Clean"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.MODEL_NAME, "Model Submission1"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.MODEL_TARGETS,
                "Event: CATEGORY, Readouts: LeadID | Email"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.NUM_SAMPLES, String
                .valueOf(NUM_SAMPLES)));
        String excludeString = Joiner.on(",").join(ModelingServiceTestUtils.createExcludeList());
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.EXCLUDE_COLUMNS, excludeString));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_TENANT, "VisiDBTest"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_URL,
                "http://localhost:8082/DLRestService"));

        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_QUERY, "Q_DataForModeling"));
        if (debug) {
            parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DEBUG, "true"));
        }
        if (!validate) {
            parameters.add(new ModelCommandParameter(command, ModelCommandParameters.VALIDATE, "false"));
        }
        return command;
    }

    public static ModelCommandId createModelCommandId() {
        DateTime dt = new DateTime(DateTimeZone.UTC);
        return new ModelCommandId(new Timestamp(dt.getMillis()), "orchestrator");
    }

    private static String getPublicIpAddress() throws SocketException {
        String result = "localhost";

        NetworkInterface ni = NetworkInterface.getByName("eth0");
        if (ni != null) {
            Enumeration<InetAddress> inetAddresses = ni.getInetAddresses();

            while (inetAddresses.hasMoreElements()) {
                InetAddress ia = inetAddresses.nextElement();
                if (!ia.isLinkLocalAddress() && !(ia instanceof Inet6Address)) {
                    result = ia.getHostAddress();
                    break;
                }
            }
        }
        return result;
    }

    private static String buildAlgorithmPropertiesStringWithFeatureThreshold(int featureThreshold) {
        if(featureThreshold > 0)
            return ALGORITHM_PROPERTIES_FEATURES_THRESHOLD + String.valueOf(featureThreshold);
        else
            return ALGORITHM_PROPERTIES_FEATURES_THRESHOLD + String.valueOf(30);
    }

}
