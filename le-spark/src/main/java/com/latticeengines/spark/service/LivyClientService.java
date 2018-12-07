package com.latticeengines.spark.service;

import org.apache.livy.scalaapi.LivyScalaClient;

public interface LivyClientService {

    LivyScalaClient createClient(String host, Iterable<String> swLibs);

}
