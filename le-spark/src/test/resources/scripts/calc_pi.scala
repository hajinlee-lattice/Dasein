val NUM_SAMPLES = scriptParams.get("NUM_SAMPLES").asInt()
val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
  val x = math.random
  val y = math.random
  x*x + y*y < 1
}.count()

// -----CELL BREAKER----

println("----- BEGIN SCRIPT OUTPUT -----")
println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")
