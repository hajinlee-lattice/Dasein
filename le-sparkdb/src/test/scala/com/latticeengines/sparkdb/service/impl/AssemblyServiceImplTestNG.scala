package com.latticeengines.sparkdb.service.impl;

import java.io.File

import scala.Array.canBuildFrom

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark._
import org.apache.spark.sql._
import org.springframework.test.context.ContextConfiguration
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import org.testng.Assert.assertTrue

import com.latticeengines.common.exposed.util.HdfsUtils
import com.latticeengines.sparkdb.functionalframework.SparkDbFunctionalTestNGBase

class AssemblyServiceImplTestNG extends SparkDbFunctionalTestNGBase {
  
  var conf = new YarnConfiguration()

  @BeforeClass(groups = Array("functional"))
  def setup() = {
    println(getClass.getResource(".").getPath())
    val inputDir = getClass.getClassLoader.getResource("com/latticeengines/sparkdb/exposed/service/impl").getPath()
    HdfsUtils.rmdir(conf, "/tmp/sources/")
    HdfsUtils.rmdir(conf, "/tmp/result")
    HdfsUtils.rmdir(conf, "/tmp/training")
    HdfsUtils.rmdir(conf, "/tmp/test")
    
    HdfsUtils.mkdir(conf, "/tmp/sources/")
    
    val files = new File(inputDir).listFiles().filter(filter)
    val copyEntries = files.map(p=>(p.getAbsolutePath(), "/tmp/sources"))
    
    doCopy(conf, copyEntries.toList)
  }
  
  def filter(fileName: File) = fileName.getAbsolutePath().endsWith(".avro")
  
  @Test(groups = Array("functional"))
  def run() {
    AssemblyServiceImpl.main(Array("1"))
    
    val sparkConf = new SparkConf(true) //
    .setMaster("local") //
    .setAppName("ParquetAvro")
    val sqc = new SQLContext(new SparkContext(sparkConf))

    val resultRdd = sqc.parquetFile("/tmp/result/part-r-00000.snappy.parquet")

    resultRdd.registerAsTable("Result")
    
    val count = sqc.sql("SELECT * FROM Result").collect().count(p=>true)
    
    println(s"Number of rows of Result parquet table = $count")
    assertTrue(count > 0)
  }


}
