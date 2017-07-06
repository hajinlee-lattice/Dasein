package com.latticeengines.perf.cli.setup.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;
import com.latticeengines.domain.exposed.modeling.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;
import com.latticeengines.perf.cli.setup.CommandLineOption;
import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.job.properties.CommandLineProperties;
import com.latticeengines.perf.job.runnable.impl.SubmitModel;

public class CommandLineSubmitModelSetup extends CommandLineSetup<SubmitModel> {

    private CommandLine cl;

    public CommandLineSubmitModelSetup(String restEndpointHost) {
        super(restEndpointHost);
    }

    public void setupOptions(String[] args) throws ParseException {
        Options ops = new Options();
        Option customer = new CommandLineOption(CUSTOMER_OPT, CUSTOMER_LONGOPT, true, true, CUSTOMER_DEF);
        Option table = new CommandLineOption(TABLE_OPT, TABLE_LONGOPT, true, true, TABLE_DEF);
        Option keyCol = new CommandLineOption(KEYCOLUMN_OPT, KEYCOLUMN_LONGOPT, true, true, KEYCOLUMN_DEF);
        Option target = new CommandLineOption(TARGET_OPT, TARGET_LONGOPT, true, true, TARGET_DEF);
        Option metadataTable = new CommandLineOption(METADATA_TABLE_OPT, METADATA_TABLE_LONGOPT, true, false,
                METADATA_TABLE_DEF);

        Option algorithmProps = new CommandLineOption(ALGORITHM_PROP_OPT, ALGORITHM_PROP_LONGOPT, true, false,
                ALGORITHM_PROP_DEF);
        algorithmProps.setValueSeparator(LIST_DELIMETER);
        algorithmProps.setArgs(Integer.MAX_VALUE);

        ops.addOption(customer).addOption(table).addOption(keyCol).addOption(target).addOption(metadataTable) //
                .addOption(algorithmProps);
        cl = clp.parse(ops, args);
    }

    @Override
    public CommandLine getCommandLine() {
        return cl;
    }

    @Override
    public Class<SubmitModel> getJobClassType() {
        return SubmitModel.class;
    }

    public void setConfiguration(SubmitModel sm) throws Exception {
        Model model = new Model();
        ModelDefinition modelDef = new ModelDefinition();

        String table = cl.getOptionValue(TABLE_OPT);
        String targets = cl.getOptionValue(TARGET_OPT);
        String keyColumns = cl.getOptionValue(KEYCOLUMN_OPT);
        String metadataTable = cl.getOptionValue(METADATA_TABLE_OPT);
        modelDef.setName("Model Definition");

        String[] algorithmProps = cl.getOptionValues(ALGORITHM_PROP_OPT);
        List<Algorithm> algorithms = new ArrayList<Algorithm>();
        if (algorithmProps != null) {
            for (String aps : algorithmProps) {
                if (StringUtils.isEmpty(aps)) {
                    continue;
                }
                String[] propList = aps.split(CommandLineProperties.VALUE_DELIMETER);
                String name = propList.length > 0 ? propList[0] : "";
                String virtualCores = propList.length > 1 ? propList[1] : "";
                String memory = propList.length > 2 ? propList[2] : "";
                String priority = propList.length > 3 ? propList[3] : "";
                if (name.equalsIgnoreCase(ALGORITHM_NAME_LR)) {
                    LogisticRegressionAlgorithm lra = new LogisticRegressionAlgorithm();
                    configAlgorithm(lra, virtualCores, memory, priority);
                    algorithms.add(lra);
                } else if (name.equalsIgnoreCase(ALGORITHM_NAME_DT)) {
                    DecisionTreeAlgorithm dta = new DecisionTreeAlgorithm();
                    configAlgorithm(dta, virtualCores, memory, priority);
                    algorithms.add(dta);
                } else if (name.equalsIgnoreCase(ALGORITHM_NAME_RF)) {
                    RandomForestAlgorithm rfa = new RandomForestAlgorithm();
                    configAlgorithm(rfa, virtualCores, memory, priority);
                    algorithms.add(rfa);
                }
            }
            modelDef.addAlgorithms(algorithms);
        }
        model.setModelDefinition(modelDef);
        model.setName("load test model for" + customer);
        model.setTable(table);
        model.setMetadataTable(metadataTable);
        // Currently we only need one target
        model.setCustomer(customer);
        model.setDataFormat(DATA_FORMAT);
        model.setKeyCols(Arrays.<String> asList(keyColumns.split(CommandLineProperties.VALUE_DELIMETER)));
        model.setTargetsList(Arrays.<String> asList(targets.split(CommandLineProperties.VALUE_DELIMETER)));

        sm.setConfiguration(restEndpointHost, model);
    }

    static void configAlgorithm(AlgorithmBase alg, String virtualCores, String memory, String priority) {
        alg.setSampleName("all");
        if (StringUtils.isEmpty(virtualCores) || StringUtils.isEmpty(memory) || StringUtils.isEmpty(priority)) {
            return;
        }
        alg.setPriority(Integer.parseInt(priority));
        alg.setContainerProperties(new StringBuilder().append("VIRTUALCORES=")//
                .append(virtualCores).append(" MEMORY=").append(memory)//
                .append(" PRIORITY=").append(priority).toString());
    }

}
