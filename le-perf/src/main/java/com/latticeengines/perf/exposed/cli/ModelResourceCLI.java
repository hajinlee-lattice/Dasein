package com.latticeengines.perf.exposed.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.springframework.web.client.RestTemplate;

import com.latticeengines.perf.domain.Algorithm;
import com.latticeengines.perf.domain.AppSubmission;
import com.latticeengines.perf.domain.DbCreds;
import com.latticeengines.perf.domain.LoadConfiguration;
import com.latticeengines.perf.domain.Model;
import com.latticeengines.perf.domain.ModelDefinition;
import com.latticeengines.perf.domain.SamplingConfiguration;
import com.latticeengines.perf.domain.SamplingElement;
import com.latticeengines.perf.domain.algorithm.AlgorithmBase;
import com.latticeengines.perf.domain.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.perf.domain.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.perf.domain.algorithm.RandomForestAlgorithm;

public class ModelResourceCLI {

    private static HashMap<String, String> optionMap = new HashMap<String, String>();
    private static List<List<String>> algList = new ArrayList<List<String>>();
    private static RestTemplate restTemplate = new RestTemplate();
    private static String DELIMETER = ",";
    private static String restHost = "localhost";

    public static void main(String[] args) throws IOException, Exception {
        if (args.length > 0)
            submitJob(args);
        else {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String line = null;
            try {
                while ((line = br.readLine()) != null) {
                    String[] command = line.split(" ");
                    submitJob(command);
                }
            } finally {
                try {
                    br.close();
                } catch (IOException e) {
                    throw e;
                }
            }
        }
    }

    private static void submitJob(String[] command) throws Exception {
        if (command.length < 7) {
            throw new Exception("Too few arguments for the legal command");
        }
        if (!command[0].equalsIgnoreCase("ledp")) {
            throw new Exception("Unrecognized command type. Please start your command with 'ledp'");
        }
        preProcessOptions(command);
        if (command[1].equalsIgnoreCase("load")) {
            loadData();
            restHost = command[2];
        } else if (command[1].equalsIgnoreCase("createsamples")) {
            createSamples();
            restHost = command[2];
        } else if (command[1].equalsIgnoreCase("submitmodel")) {
            submitModel(command[2]);
            restHost = command[3];
        } else {
            throw new Exception("Unsupported command. Please check the user doc.");
        }
    }

    private static void submitModel(String modelName) throws Exception {
        Model model = new Model();
        ModelDefinition modelDef = new ModelDefinition();
        String customer = optionMap.get("c");
        String table = optionMap.get("t");
        String features = optionMap.get("f");
        String targets = optionMap.get("T");
        String keyColumns = optionMap.get("kc");
        modelDef.setName("Model Definition");
        List<Algorithm> algorithms = new ArrayList<Algorithm>();
        for (List<String> opList : algList) {
            String name = opList.get(0);
            String virtualCores = opList.get(1);
            String memory = opList.get(2);
            String priority = opList.get(3);
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
                algorithms.add(rfa);
            }
        }
        modelDef.setAlgorithms(algorithms);
        model.setModelDefinition(modelDef);
        model.setName(modelName);
        model.setTable(table);
        model.setFeatures(Arrays.<String> asList(features.split(DELIMETER)));
        model.setTargets(Arrays.<String> asList(targets.split(DELIMETER)));
        model.setCustomer(customer);
        model.setDataFormat("avro");
        model.setKeyCols(Arrays.<String> asList(keyColumns.split(DELIMETER)));
        AppSubmission submission = restTemplate.postForObject("http://" + restHost + ":8080/rest/submit", model,
                AppSubmission.class, new Object[] {});
        optionMap.clear();
        algList.clear();
        System.out.println(submission.getApplicationIds());
    }

    private static void configAlgorithm(AlgorithmBase alg, String virtualCores, String memory, String priority) {
        alg.setSampleName("s" + priority);
        alg.setPriority(Integer.parseInt(priority));
        alg.setContainerProperties(new StringBuilder().append("VIRTUALCORES=")//
                .append(virtualCores).append(" MEMORY=").append(memory)//
                .append(" PRIORITY=").append(priority).toString());
    }

    private static void createSamples() throws Exception {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        String customer = optionMap.get("c");
        String table = optionMap.get("t");
        String trainingPercentage = optionMap.get("tp");
        String numOfSamples = optionMap.get("N");
        if (customer == null || table == null || trainingPercentage == null || numOfSamples == null) {
            throw new Exception("Missing argument!");
        }
        samplingConfig.setCustomer(customer);
        samplingConfig.setTable(table);
        samplingConfig.setTrainingPercentage(Integer.parseInt(trainingPercentage));
        int num = Integer.parseInt(numOfSamples), value = 100 / num;
        SamplingElement se = null;
        for (int i = 0; i < num - 1; i++) {
            String name = "s" + i;
            se = new SamplingElement();
            se.setName(name);
            se.setPercentage(value);
            samplingConfig.addSamplingElement(se);
            value *= i + 2;
        }
        se = new SamplingElement();
        se.setName("all");
        se.setPercentage(100);
        samplingConfig.addSamplingElement(se);
        AppSubmission submission = restTemplate.postForObject("http://" + restHost + ":8080/rest/createSamples",
                samplingConfig, AppSubmission.class, new Object[] {});
        optionMap.clear();
        System.out.println(submission.getApplicationIds());
    }

    private static void loadData() throws Exception {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        String host = optionMap.get("H");
        String port = optionMap.get("P");
        String db = optionMap.get("db");
        String user = optionMap.get("u");
        String passwd = optionMap.get("ps");
        String customer = optionMap.get("c");
        String table = optionMap.get("t");
        String keyCol = optionMap.get("kc");
        if (host == null || port == null || db == null || user == null || passwd == null || customer == null
                || table == null || keyCol == null) {
            throw new Exception("Missing argument!");
        }
        builder.host(host).port(Integer.parseInt(port)).db(db).user(user).password(passwd);
        DbCreds dc = new DbCreds(builder);
        config.setCustomer(customer);
        config.setTable(table);
        config.setKeyCols(Arrays.<String> asList(keyCol.split(DELIMETER)));
        config.setCreds(dc);
        AppSubmission submission = restTemplate.postForObject("http://" + restHost + ":8080/rest/load", config,
                AppSubmission.class, new Object[] {});
        optionMap.clear();
        System.out.println(submission.getApplicationIds());
    }

    private static void preProcessOptions(String[] args) {
        int count = 0;
        String prevStr = null;
        boolean subOption = false;
        List<String> opList = null;
        for (String s : args) {
            if (s.equalsIgnoreCase("--alg")) {
                opList = new ArrayList<String>();
                algList.add(opList);
                subOption = true;
            } else if (s.startsWith("-")) {
                prevStr = s;
            } else if (prevStr != null) {
                if (!subOption) {
                    optionMap.put(prevStr.substring(1), s);
                    // System.out.println(prevStr.substring(1) + s);
                } else {
                    if (prevStr.equalsIgnoreCase("-n")) {
                        opList.add(0, s);
                    } else if (prevStr.equalsIgnoreCase("-vc")) {
                        opList.add(1, s);
                    } else if (prevStr.equalsIgnoreCase("-m")) {
                        opList.add(2, s);
                    } else if (prevStr.equalsIgnoreCase("-p")) {
                        opList.add(3, s);
                    }
                    count++;
                    if (count == 4) {
                        subOption = false;
                        prevStr = null;
                        count = 0;
                    }
                }
            }
        }
    }
}
