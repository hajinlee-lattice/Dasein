package com.latticeengines.sparkdb.operator

import java.util.HashMap
import java.util.Map

import com.latticeengines.domain.exposed.dataplatform.{HasProperty => DomainHasProperty}

trait HasProperty extends DomainHasProperty {

  private var map: Map[String, Object] = new HashMap[String, Object]()
  
  override def getPropertyValue(key: String): Object = {
    map.get(key)
  }
  
  override def setPropertyValue(key: String, value: Any) = {
    map.put(key, value.asInstanceOf[Object])
  }
}