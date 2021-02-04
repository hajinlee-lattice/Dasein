package com.latticeengines.spark.util

import com.latticeengines.common.exposed.util.CipherUtils
import com.latticeengines.domain.exposed.metadata.{InterfaceName, TableRoleInCollection}
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn
import org.apache.spark.sql.DataFrame

private [spark] object ElasticSearchUtils {

  val entityId = InterfaceName.EntityId.name
  val accountId = InterfaceName.AccountId.name
  val contactId = InterfaceName.ContactId.name
  val lookupKey = InterfaceName.AtlasLookupKey.name
  val recordId = TimelineStandardColumn.RecordId.getColumnName
  val sortKey = InterfaceName.SortKey.name

  def addPrefix(df: DataFrame, prefix: String): DataFrame = {
    val role = TableRoleInCollection.getByName(prefix).ordinal.toString
    val cols = df.columns.toSeq
    df.select(cols.map(c => {
      if (c.equals(accountId) || c.equals(contactId) || c.equals(entityId)) {
        df.col(c)
      } else {
        df.col(c).as(role + ":" + c)
      }
    }): _*)
  }

  def getBaseConfig(host: String, port: String, user: String, password: String, encryptionKey: String, salt: String)
  : Map[String, String] = {
    val passwd : String = CipherUtils.decrypt(password, encryptionKey, salt);
    Map(
      "es.write.operation" -> "upsert",
      "es.nodes.wan.only" -> "true",
      "es.index.auto.create" -> "false",
      "es.batch.write.refresh" -> "false",
      //      "es.batch.size.bytes" -> "10mb",
      "es.nodes" -> host,
      "es.port" -> port,
      "es.net.http.auth.user" -> user,
      "es.net.http.auth.pass" -> passwd,
      "es.net.ssl" -> "true")
  }

  def constructIndexName(customerSpace: String, entity: String, signature: String) : String = {
    String.format("%s_%s_%s", customerSpace, entity, signature).toLowerCase
  }
}
