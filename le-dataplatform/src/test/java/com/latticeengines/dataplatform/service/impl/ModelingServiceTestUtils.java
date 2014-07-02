package com.latticeengines.dataplatform.service.impl;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import com.latticeengines.dataplatform.service.impl.dlorchestration.ModelCommandParameters;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;

public class ModelingServiceTestUtils {

    public static final int NUM_SAMPLES = 1;

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

    public static ModelCommand createModelCommandWithCommandParameters() {
        List<ModelCommandParameter> parameters = new ArrayList<>();
        ModelCommand command = new ModelCommand(1L, "Nutanix", ModelCommandStatus.NEW, parameters, ModelCommand.TAHOE);
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DEPIVOTED_EVENT_TABLE, "Q_EventTableDepivot_Nutanix"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.EVENT_TABLE, "Q_EventTable_Nutanix"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.KEY_COLS, "Nutanix_EventTable_Clean"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.MODEL_NAME, "Model Submission1"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.MODEL_TARGETS, "P1_Event"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.NUM_SAMPLES, String.valueOf(NUM_SAMPLES)));
        String excludeString = Joiner.on(",").join(ModelingServiceTestUtils.createExcludeList());
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.EXCLUDE_COLUMNS, excludeString));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_TENANT, "ADEBD2V67059448rX25059174r"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_URL, "http://10.41.1.238/"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_USERNAME, "someperson@lattice-engines.com"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_PASSWORD, "somepersonpassword"));
        parameters.add(new ModelCommandParameter(command, ModelCommandParameters.DL_TOKEN, "somepersontoken"));

        return command;
    }
}
