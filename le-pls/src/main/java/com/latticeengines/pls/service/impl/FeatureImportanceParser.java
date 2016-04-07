package com.latticeengines.pls.service.impl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;

@Component("featureImportanceParser")
public class FeatureImportanceParser {

    @Value("${pls.modelingservice.basedir}")
    private String modelServiceHdfsBaseDir;

    private Configuration yarnConfiguration;
    
    public FeatureImportanceParser() {
    }

    public FeatureImportanceParser(String hdfsHostPort) {
        this.modelServiceHdfsBaseDir = "hdfs://" + hdfsHostPort + "/user/s-analytics/customers";
        this.yarnConfiguration = new Configuration();
        yarnConfiguration.set("fs.defaultFS", hdfsHostPort);
    }

    public void parse(CommandLine commandLine) throws IOException {
        String inputFile = commandLine.getOptionValue("inputfile");
        String outputFile = commandLine.getOptionValue("outputfile");

        try (BufferedReader reader = new BufferedReader(new FileReader(new File(inputFile)))) {
            String line = null;
            boolean first = true;
            while ((line = reader.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;
                }
                String[] tokens = line.split(",");
                String tenant = tokens[0];
                String modelId = tokens[1];

                System.out.println(String.format("Processing tenant %s and model id %s", tenant, modelId));
                FileStatus hdfsPath = findHdfsPath(tenant, modelId);
                if (hdfsPath == null) {
                    hdfsPath = findHdfsPath(String.format("%s.%s.Production", tenant, tenant), modelId);
                }

                if (hdfsPath == null) {
                    System.err.println(String.format("No feature importance file for tenant %s with model id %s", tenant, modelId));
                    continue;
                }
                System.out.println(String.format("Found HDFS path for tenant %s and model id %s", tenant, modelId));
                try (FileWriter writer = new FileWriter(new File(outputFile))) {
                    writer.write("Tenant,ModelId,Feature,Importance\n");
                    String path = hdfsPath.getPath().toString();
                    try {
                        Map<String, Double> fiMap = parse(path, HdfsUtils.getHdfsFileContents(yarnConfiguration, path));
                        System.out.println(String.format("Retrieved contents for tenant %s and model id %s", tenant, modelId));
                        for (Map.Entry<String, Double> entry : fiMap.entrySet()) {
                            writer.write(String.format("%s,%s,%s,%s,%f\n", //
                                    tenant, //
                                    modelId, //
                                    convertTime(hdfsPath.getModificationTime()), //
                                    entry.getKey(), //
                                    entry.getValue()));
                        }
                    } catch (Exception e) {
                        System.err.println(ExceptionUtils.getStackTrace(e));
                    }
                }
            }
        }
    }
    
    private String convertTime(long time) {
        Date date = new Date(time);
        Format format = new SimpleDateFormat("YYYY-MM-dd");
        return format.format(date);
    }

    private FileStatus findHdfsPath(String tenant, String modelId) {
        String startingHdfsPoint = modelServiceHdfsBaseDir + "/" + tenant + "/models";
        HdfsUtils.HdfsFileFilter filter = new HdfsUtils.HdfsFileFilter() {

            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
                    return false;
                }

                String name = file.getPath().getName().toString();
                return name.equals("rf_model.txt");
            }

        };

        try {
            List<FileStatus> files = HdfsUtils.getFileStatusesForDirRecursive(yarnConfiguration, startingHdfsPoint, filter);
            modelId = UuidUtils.extractUuid(modelId);
            for (FileStatus file : files) {
                String uuid = UuidUtils.parseUuid(file.getPath().toString());

                if (uuid.equals(modelId)) {
                    return file;
                }
            }
        } catch (IOException e) {
            return null;
        }
        return null;
    }

    private Map<String, Double> parse(Reader reader) throws IOException {
        Map<String, Double> fiMap = new HashMap<>();
        try (BufferedReader r = new BufferedReader(reader)) {
            String line = null;
            boolean first = true;
            while ((line = r.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;
                }
                String[] tokens = line.split(",");
                if (tokens.length >= 2) {
                    fiMap.put(tokens[0], Double.valueOf(tokens[1].trim()));
                }
            }
        }
        return fiMap;
    }

    public Map<String, Double> parse(String hdfsPath, String fileContents) throws IOException {
        return parse(new InputStreamReader(new ByteArrayInputStream(fileContents.getBytes())));
    }

    public static void main(String[] args) {
        CommandLineParser parser = new GnuParser();
        Options options = new Options();
        options.addOption("o", "outputfile", true, "the output file with the top features");
        options.addOption("i", "inputfile", true, "the input file with the pair of tenant and model guid");
        options.addOption("h", "hdfs-hostport", true, "the HDFS host port");

        try {
            CommandLine commandLine = parser.parse(options, args);

            FeatureImportanceParser featureParser = new FeatureImportanceParser(
                    commandLine.getOptionValue("hdfs-hostport"));
            featureParser.parse(commandLine);
        } catch (ParseException | IOException e) {
            throw new RuntimeException(e);
        }

    }
}
