//package com.latticeengines.eai.service.impl.file.strategy;
//
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.text.ParseException;
//import java.util.List;
//import java.util.Locale;
//import java.util.Properties;
//import java.util.Set;
//
//import org.apache.avro.Schema;
//import org.apache.avro.Schema.Type;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.commons.collections.CollectionUtils;
//import org.apache.commons.csv.CSVFormat;
//import org.apache.commons.csv.CSVParser;
//import org.apache.commons.csv.CSVRecord;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.yarn.api.records.ApplicationId;
//import org.apache.hadoop.yarn.util.ConverterUtils;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.broadcast.Broadcast;
//import org.apache.spark.launcher.SparkAppHandle;
//import org.apache.spark.launcher.SparkLauncher;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.format.number.NumberFormatter;
//import org.springframework.stereotype.Component;
//
//import com.google.common.annotations.VisibleForTesting;
//import com.latticeengines.common.exposed.csv.LECSVFormat;
//import com.latticeengines.common.exposed.util.JsonUtils;
//import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
//import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
//import com.latticeengines.domain.exposed.metadata.Attribute;
//import com.latticeengines.domain.exposed.metadata.InterfaceName;
//import com.latticeengines.domain.exposed.metadata.LogicalDataType;
//import com.latticeengines.domain.exposed.metadata.Table;
//import com.latticeengines.domain.exposed.metadata.validators.InputValidator;
//import com.latticeengines.domain.exposed.util.TableUtils;
//
//@Component("sparkImport")
//public class SparkImport {
//
//    @Autowired
//    private Configuration yarnConfiguration;
//
//    public ApplicationId launch(Properties props) {
//        SparkAppHandle spark = null;
//        props.put(MapReduceProperty.INPUT.name(), "hdfs://" + props.getProperty(MapReduceProperty.INPUT.name()));
//        try {
//            spark = new SparkLauncher().setMainClass("com.latticeengines.eai.service.impl.file.strategy.SparkImportJob")
//                    .setAppResource("hdfs:///app/eai/lib/le-eai-3.1.0-SNAPSHOT-shaded.jar")
//                    .setMaster("yarn")
//                    .setSparkHome("/home/hliu/app/spark-2.0.1-bin-hadoop2.7")
//                    .setConf("spark.props", JsonUtils.serialize(props))
//                    // .setConf("spark.yarn.config",
//                    //.setConf("spark.rpc.message.maxSize", "300")
//                   // .setConf("spark.memory.fraction", "0.8")
//                    .setConf("spark.executor.instances", "1")
//                    .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
//                    .setConf(SparkLauncher.EXECUTOR_MEMORY, "1g")
//                    .setDeployMode("cluster")
//                    .setConf("spark.hadoop.yarn.timeline-service.enabled", "false")
//                    .setConf("spark.eventLog.enabled", "true")
//                    .setConf("spark.eventLog.dir", "hdfs://namenode/shared/spark-logs")
//                  //  .setConf("spark.rpc.netty.dispatcher.numThreads","2")
//                 //   .setConf("spark.executor.heartbeatInterval", "200s")
//                    .startApplication();
//            Thread.sleep(15000L);
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//
//        return ConverterUtils.toApplicationId(spark.getAppId());
//
//        // BufferedReader reader = new BufferedReader(new
//        // InputStreamReader(spark.getInputStream()));
//        // String line = null;
//        // while ((line = reader.readLine()) != null) {
//        // System.out.println(line);
//        // }
//        //
//        // BufferedReader reader2 = new BufferedReader(new
//        // InputStreamReader(spark.getErrorStream()));
//        // String line2 = null;
//        // while ((line2 = reader2.readLine()) != null) {
//        // System.out.println(line2);
//        // }
//
//    }
//
//}
