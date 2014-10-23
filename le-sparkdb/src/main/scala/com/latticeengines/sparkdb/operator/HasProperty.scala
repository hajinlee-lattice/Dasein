package com.latticeengines.sparkdb.operator

import java.util.HashMap
import java.util.Map
import java.util.Map.Entry
import java.util.Set

import com.latticeengines.domain.exposed.dataplatform.{HasProperty => DomainHasProperty}

trait HasProperty extends DomainHasProperty {

  private var map: Map[String, Any] = new HashMap[String, Any]()
  
  override def getPropertyValue(key: String): Object = map.get(key).asInstanceOf[Object]
  
  override def setPropertyValue(key: String, value: Any) = map.put(key, value)
  
  override def getEntries(): Set[Entry[String, Object]] = map.entrySet().asInstanceOf[Set[Entry[String, Object]]]

}