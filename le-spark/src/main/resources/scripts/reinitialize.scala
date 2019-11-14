rawInput = mapper.readValue[List[JsonNode]]("""{{INPUT}}""")

scriptInput = rawInput map { input => {
  val storage = input.get("StorageType").asText().toLowerCase()
  storage match {
    case "hdfs" => loadHdfsUnit(input)
    case _ => throw new UnsupportedOperationException(s"Unknown storage $storage")
  }
}
}

println("----- BEGIN SCRIPT OUTPUT -----")
println(s"Input: $scriptInput")
println("----- END SCRIPT OUTPUT -----")

scriptParams = mapper.readValue[JsonNode]("""{{PARAMS}}""")

println("----- BEGIN SCRIPT OUTPUT -----")
println(s"Params: $scriptParams")
println("----- END SCRIPT OUTPUT -----")

scriptTargets = mapper.readValue[List[ObjectNode]]("""{{TARGETS}}""")

println("----- BEGIN SCRIPT OUTPUT -----")
println(s"Targets: $scriptTargets")
println("----- END SCRIPT OUTPUT -----")

lattice = new LatticeContext(scriptInput, scriptParams, scriptTargets)
