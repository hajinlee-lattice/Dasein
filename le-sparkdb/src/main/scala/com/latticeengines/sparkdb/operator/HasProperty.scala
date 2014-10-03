package com.latticeengines.sparkdb.operator

import java.util.HashMap
import java.util.Map

import com.latticeengines.domain.exposed.dataplatform.{HasProperty => DomainHasProperty}

trait HasProperty extends DomainHasProperty {

  private var map: Map[String, Any] = new HashMap[String, Any]()
  
  override def getPropertyValue(key: String): Object = map.get(key).asInstanceOf[Object]
  
  override def setPropertyValue(key: String, value: Any) = map.put(key, value)

}