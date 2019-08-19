// ============
// BEGIN SCRIPT
// ============
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column

// read input, table name = eventTable
val eventTable: DataFrame = lattice.input(0)

// read config
val latticeAccId = lattice.params.get("LID_FIELD").asText()
val latticeId = lattice.params.get("INT_LDC_LID").asText()
val eventCol =  lattice.params.get("EVENT").asText()
val intLdcRemoveCol = lattice.params.get("INT_LDC_REMOVED").asText()
val intLdcDedupeIdCol = lattice.params.get("INT_LDC_DEDUPE_ID").asText()
val sort_event = "__Sort_Event__"

// computation
// filter based on INT_LDC_REMOVED column
val dfEventTableFilter = eventTable.filter(eventTable(intLdcRemoveCol)===0)
// add sortEvent column mapped to Event : true = 1 and false = 0
val dfFalseEvent = dfEventTableFilter.filter(eventTable(eventCol)===false).withColumn(sort_event, lit(0))
val dfTrueEvent = dfEventTableFilter.filter(eventTable(eventCol)===true).withColumn(sort_event, lit(1))
// merged output with new column : sortEvent
val dfMergedEventOutput = dfFalseEvent.union(dfTrueEvent)
// df with dedupId present and absent
val dfHasDedupeId = dfMergedEventOutput.filter(eventTable(intLdcDedupeIdCol).isNotNull)
val dfNoDedupeId = dfMergedEventOutput.filter(eventTable(intLdcDedupeIdCol).isNull)
// dedup based on : group by INT_LDC_DEDUPE_ID column and sort by sortevent column in desc order
val window_DedupeIdGroupSortDescCol = Window.partitionBy(eventTable(intLdcDedupeIdCol)).orderBy($"__Sort_Event__".desc)
val dfMergedEventOutputDeduped = dfMergedEventOutput.withColumn("row_num", row_number() over window_DedupeIdGroupSortDescCol).where($"row_num" === 1).drop("row_num")
val dfCombinedOutput = dfMergedEventOutputDeduped.union(dfNoDedupeId)
// dedup by ID by ordering by sort_event column
val window_DedupeById = Window.partitionBy($"ID").orderBy($"__Sort_Event__".desc)
val finalDedupedOutput = dfCombinedOutput.withColumn("row_num", row_number() over window_DedupeById).where($"row_num" === 1).drop("row_num")
// Remove Internal Attributes
val colsToRemove = Seq("__LDC_DedupeId__", "__LDC_Removed__", "__Sort_Event__") 
val finalRemovedIntAttrs = finalDedupedOutput.select(finalDedupedOutput.columns.filter(colName => !colsToRemove.contains(colName)).map(colName => new Column(colName)): _*) 
// write output
lattice.output = finalRemovedIntAttrs::Nil
lattice.outputStr = "This is my output!"