package com.latticeengines.perf.job.runnable.commandline.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.latticeengines.domain.exposed.dataplatform.Algorithm;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;
import com.latticeengines.domain.exposed.dataplatform.algorithm.AlgorithmBase;
import com.latticeengines.domain.exposed.dataplatform.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.dataplatform.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.domain.exposed.dataplatform.algorithm.RandomForestAlgorithm;
import com.latticeengines.perf.cli.setup.CommandLineOption;
import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.job.properties.CommandLineProperties;
import com.latticeengines.perf.job.runnable.impl.SubmitModel;

public class CommandLineSubmitModel extends SubmitModel implements CommandLineSetup {

    private CommandLine cl;

    public CommandLineSubmitModel() {
    }

    public CommandLineSubmitModel(CommandLine cl, String customer, String restEndpointHost) {
        super(customer, restEndpointHost);
        this.cl = cl;
    }

    @Override
    public Model setConfiguration() throws Exception {
        Model model = new Model();
        ModelDefinition modelDef = new ModelDefinition();
        String table = cl.getOptionValue("t");
        String targets = cl.getOptionValue("T");
        String keyColumns = cl.getOptionValue("kc");
        String metadataTable = cl.getOptionValue("mt");
        modelDef.setName("Model Definition");

        String[] algorithmProps = cl.getOptionValues("alg");
        List<Algorithm> algorithms = new ArrayList<Algorithm>();
        for (String aps : algorithmProps) {
            String[] propList = aps.split(CommandLineProperties.DELIMETER);
            String name = propList[0];
            String virtualCores = propList[1];
            String memory = propList[2];
            String priority = propList[3];
            if (name.equalsIgnoreCase("lr")) {
                LogisticRegressionAlgorithm lra = new LogisticRegressionAlgorithm();
                configAlgorithm(lra, virtualCores, memory, priority);
                algorithms.add(lra);
            } else if (name.equalsIgnoreCase("dt")) {
                DecisionTreeAlgorithm dta = new DecisionTreeAlgorithm();
                configAlgorithm(dta, virtualCores, memory, priority);
                algorithms.add(dta);
            } else if (name.equalsIgnoreCase("rf")) {
                RandomForestAlgorithm rfa = new RandomForestAlgorithm();
                configAlgorithm(rfa, virtualCores, memory, priority);
                rfa.setAlgorithmProperties("criterion=gini n_estimators=100 n_jobs=4 min_samples_split=25 min_samples_leaf=10 bootstrap=True");
                algorithms.add(rfa);
            }
        }
        modelDef.addAlgorithms(algorithms);
        model.setModelDefinition(modelDef);
        model.setName("load test model for" + customer);
        model.setTable(table);
        model.setMetadataTable(metadataTable);
        // Currently we only need one target
        model.setCustomer(customer);
        model.setDataFormat("avro");
        model.setKeyCols(Arrays.<String> asList(keyColumns.split(CommandLineProperties.DELIMETER)));
        model.setTargetsList(Arrays.<String> asList(targets.split(CommandLineProperties.DELIMETER)));
        model.setFeaturesList(rc.getFeatures(model));
        return model;
    }

    private void configAlgorithm(AlgorithmBase alg, String virtualCores, String memory, String priority) {
        alg.setSampleName("s" + priority);
        alg.setPriority(Integer.parseInt(priority));
        alg.setContainerProperties(new StringBuilder().append("VIRTUALCORES=")//
                .append(virtualCores).append(" MEMORY=").append(memory)//
                .append(" PRIORITY=").append(priority).toString());
    }

    public void setupOptions(String[] args) throws ParseException {
        Options ops = new Options();
        Option customer = new CommandLineOption("c", "customer", true, true, "Number OF Customers sending requests");
        Option table = new CommandLineOption("t", "table", true, true, "Table Name");
        Option keyCol = new CommandLineOption("kc", "keycolumn", true, true, "Key Column Name");
        Option target = new CommandLineOption("T", "target", true, true, "Target Column Name");
        Option metadataTable = new CommandLineOption("mt", "metadatatable", true, false, "Metadata Table Name");

        Option algorithmProps = new CommandLineOption("alg", "algorithmproperties", true, true,
                "Algorithm Properties(Algorithm Name, virtual cores, memory, priority)");
        algorithmProps.setValueSeparator(DELIMETER);
        algorithmProps.setArgs(Integer.MAX_VALUE);

        ops.addOption(customer).addOption(table).addOption(keyCol).addOption(target).addOption(metadataTable) //
                .addOption(algorithmProps);
        cl = clp.parse(ops, args);
    }

    @Override
    public CommandLine getCommandLine() {
        return cl;
    }

}
