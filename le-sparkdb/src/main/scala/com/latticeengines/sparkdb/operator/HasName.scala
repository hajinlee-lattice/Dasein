package com.latticeengines.sparkdb.operator

import com.latticeengines.domain.exposed.dataplatform.{HasName => DomainHasName}
import org.apache.spark.rdd.RDD

trait HasName extends DomainHasName {

  private var name: String = ""
  
  override def getName(): String = {
    name
  }

  override def setName(name: String) = {
    this.name = name;
  }
  
}