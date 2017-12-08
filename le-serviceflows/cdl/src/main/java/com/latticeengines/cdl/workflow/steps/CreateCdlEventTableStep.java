package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CreateCdlEventTableParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;
import com.latticeengines.serviceflows.workflow.util.TableCloneUtils;

@Component("createCdlEventTableStep")
public class CreateCdlEventTableStep extends RunDataFlow<CreateCdlEventTableConfiguration> {

    private static Logger log = LoggerFactory.getLogger(CreateCdlEventTableStep.class);
    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Value("${dataplatform.queue.scheme}")
    private String queueScheme;

    private String clonedApsTableName;

    @Override
    public void onConfigurationInitialized() {
        CreateCdlEventTableConfiguration configuration = getConfiguration();
        configuration.setApplyTableProperties(true);
        configuration.setDataFlowParams(createDataFlowParameters());
    }

    private DataFlowParameters createDataFlowParameters() {
        configuration.setTargetTableName(configuration.getOutputTableName());
        Table inputTable = getAndSetInputTable();
        Table apsTable = getAndSetApsTable();
        Table accountTable = getAndSetAccountTable();
        CreateCdlEventTableParameters parameters = new CreateCdlEventTableParameters(inputTable.getName(),
                apsTable.getName(), accountTable.getName());
        return parameters;
    }

    private Table getAndSetAccountTable() {
        Table accountTable = dataCollectionProxy.getTable(getConfiguration().getCustomerSpace().toString(),
                TableRoleInCollection.ConsolidatedAccount);
        if (accountTable == null) {
            throw new RuntimeException("There's no Account table!");
        }
        int changedCount = 0;
        List<Attribute> attributes = accountTable.getAttributes();
        List<String> internal = Arrays.asList(ModelingMetadata.INTERNAL_TAG);
        for (Attribute attribute : attributes) {
            if (CollectionUtils.isEmpty(attribute.getTags()) || attribute.getTags().get(0).equals("")) {
                attribute.setTags(internal);
                changedCount++;
            }
        }
        if (changedCount > 0)
            metadataProxy.updateTable(configuration.getCustomerSpace().toString(), accountTable.getName(),
                    accountTable);
        log.info("The number of attributes having no Tags is=" + changedCount);
        return accountTable;
    }

    private Table getAndSetApsTable() {
        String customerSpace = configuration.getCustomerSpace().toString();
        Table apsTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.AnalyticPurchaseState);
        if (apsTable == null) {
            throw new RuntimeException("There's no AnalyticPurchaseState table!");
        }
        clonedApsTableName = NamingUtils.timestamp(TableRoleInCollection.AnalyticPurchaseState.name());
        String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        queue = LedpQueueAssigner.overwriteQueueAssignment(queue, queueScheme);
        Table clonedApsTable = TableCloneUtils.cloneDataTable(yarnConfiguration, configuration.getCustomerSpace(),
                clonedApsTableName, apsTable, queue);
        List<Attribute> attributes = clonedApsTable.getAttributes();
        for (Attribute attribute : attributes) {
            String name = attribute.getName();
            if ("LEAccount_ID".equalsIgnoreCase(name) || "Period_ID".equalsIgnoreCase(name)
                    || InterfaceName.AnalyticPurchaseState_ID.name().equalsIgnoreCase(name)) {
                attribute.setApprovedUsage(ApprovedUsage.NONE);
                attribute.setTags(ModelingMetadata.EXTERNAL_TAG);
                attribute.setCategory(Category.ACCOUNT_INFORMATION);
                if (InterfaceName.AnalyticPurchaseState_ID.name().equalsIgnoreCase(name)) {
                    attribute.setLogicalDataType(LogicalDataType.InternalId);
                }
            } else {
                attribute.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
                attribute.setCategory(Category.ACCOUNT_INFORMATION);
                attribute.setDisplayDiscretizationStrategy("{\"unified\": {}}");
                attribute.setTags(ModelingMetadata.INTERNAL_TAG);
                setDisplayNameAndOthers(attribute, name);
            }
        }
        metadataProxy.createTable(customerSpace, clonedApsTableName, clonedApsTable);
        return metadataProxy.getTable(customerSpace, clonedApsTableName);
    }

    private Table getAndSetInputTable() {
        Table inputTable = getObjectFromContext(FILTER_EVENT_TABLE, Table.class);
        if (inputTable == null) {
            throw new RuntimeException("There's no input table found!");
        }
        List<Attribute> attributes = inputTable.getAttributes();
        for (Attribute attribute : attributes) {
            attribute.setApprovedUsage(ApprovedUsage.NONE);
            attribute.setTags(ModelingMetadata.EXTERNAL_TAG);
            String name = attribute.getName();
            if (InterfaceName.Target.name().equalsIgnoreCase(name)
                    || InterfaceName.Event.name().equalsIgnoreCase(name)) {
                attribute.setLogicalDataType(LogicalDataType.Event);
            }
        }
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), inputTable.getName(), inputTable);
        return inputTable;
    }

    protected void setDisplayNameAndOthers(Attribute attribute, String name) {
        if (name.matches("Product_.*_Revenue")) {
            Pattern pattern = Pattern.compile("Product_(.*)_Revenue");
            Matcher matcher = pattern.matcher(name);
            matcher.matches();
            attribute.setDisplayName("Last Period Spend for " + matcher.group(1));
            attribute.setDescription("Product spend in last period");
            attribute.setFundamentalType(FundamentalType.CURRENCY);
            return;
        }
        if (name.matches("Product_.*_RevenueRollingSum6")) {
            Pattern pattern = Pattern.compile("Product_(.*)_RevenueRollingSum6");
            Matcher matcher = pattern.matcher(name);
            matcher.matches();
            attribute.setDisplayName("6-Period Spend for " + matcher.group(1));
            attribute.setDescription("Product spend for last 6 periods");
            attribute.setFundamentalType(FundamentalType.CURRENCY);
            return;
        }
        if (name.matches("Product_.*_RevenueMomentum3")) {
            Pattern pattern = Pattern.compile("Product_(.*)_RevenueMomentum3");
            Matcher matcher = pattern.matcher(name);
            matcher.matches();
            attribute.setDisplayName("Rate of Change of 3-Period Spend for " + matcher.group(1));
            attribute.setDescription(
                    "Percent change in the 3 period spend, where values > 0 show increasing spend and < 0 indicate decreasing spend");
            return;
        }
        if (name.matches("Product_.*_Units")) {
            Pattern pattern = Pattern.compile("Product_(.*)_Units");
            Matcher matcher = pattern.matcher(name);
            matcher.matches();
            attribute.setDisplayName("Last Period Units for " + matcher.group(1));
            attribute.setDescription("Units purchased in last period");
            return;
        }
        if (name.matches("Product_.*_Span")) {
            Pattern pattern = Pattern.compile("Product_(.*)_Span");
            Matcher matcher = pattern.matcher(name);
            matcher.matches();
            attribute.setDisplayName("Purchase Recency for " + matcher.group(1));
            attribute.setDescription(
                    "Indicator of how recently a customer purchased, where higher values indicate more recent purchase (e.g. 1 = Last Period  and 0 = Never)");
            return;
        }
    }

    @Override
    public void onExecutionCompleted() {
        Table eventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(configuration.getOutputTableName(), eventTable);
        putStringValueInContext(MATCH_FETCH_ONLY, "true");

        metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), clonedApsTableName);
    }

}
