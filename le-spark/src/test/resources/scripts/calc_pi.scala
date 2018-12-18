val NUM_SAMPLES = lattice.params.get("NUM_SAMPLES").asInt()
val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
  val x = math.random
  val y = math.random
  x*x + y*y < 1
}.count()

lattice.outputStr = s"Pi is roughly ${4.0 * count / NUM_SAMPLES}"
