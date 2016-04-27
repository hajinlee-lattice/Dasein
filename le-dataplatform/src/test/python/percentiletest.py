import glob
import json
import sys

from trainingtestbase import TrainingTestBase

class PercentileTest(TrainingTestBase):

	percentileBucketsNum = 100
	sampleLeadNum = 10
	maxPercentileScore = 99
	minPercentileScore = 5

	def testPercentile(self):
		if 'launcher' in sys.modules:
			del sys.modules['launcher']
		from launcher import Launcher

		traininglaucher = Launcher("99_5_metadata-modeling.json")
		traininglaucher.execute(False)


		# Retrieve the model json file and assert the percentiles are correct
		jsonDict = json.loads(open(glob.glob("./results/*.json")[0]).read())

		percentileNum = len(jsonDict["PercentileBuckets"])
		print percentileNum
		self.assertEquals(percentileNum, self.percentileBucketsNum)
		for index in range(0, percentileNum):
			percentileScore = jsonDict["PercentileBuckets"][index]["Percentile"]
			self.assertTrue(percentileScore <= self.maxPercentileScore and percentileScore >= self.minPercentileScore)

		# Retrieve the modelsummary json file and assert the top and bottom leads are correct
		modelsummary = json.loads(open(glob.glob("./results/enhancements/modelsummary.json")[0]).read())
		topSamples = modelsummary["TopSample"]
		bottomSamples = modelsummary["BottomSample"]
		self.assertEquals(len(topSamples), self.sampleLeadNum)
		self.assertEquals(len(bottomSamples), self.sampleLeadNum)

		for index in range(0, self.sampleLeadNum):
			topScore = topSamples[index]["Score"]
			bottomScore = bottomSamples[index]["Score"]
			self.assertEquals(topScore, self.maxPercentileScore)
			self.assertEquals(bottomScore, self.minPercentileScore)

