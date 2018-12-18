import random

NUM_SAMPLES = lattice.params['NUM_SAMPLES']

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

# -----CELL BREAKER----

count = sc.parallelize(range(0, NUM_SAMPLES)).filter(inside).count()

lattice.output_str = "Pi is roughly %f" % (4.0 * count / NUM_SAMPLES)
