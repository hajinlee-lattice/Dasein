package com.latticeengines.sparkdb.operator

import java.util.HashMap
import java.util.Map

import com.latticeengines.domain.exposed.dataplatform.{HasProperty => DomainHasProperty}

trait HasProperty extends DomainHasProperty {

  private var map: Map[String, String] = new HashMap[String, String]()
  
  override def getPropertyValue(key: String): String = map.get(key)
  
  override def setPropertyValue(key: String, value: String) = map.put(key, value)

}