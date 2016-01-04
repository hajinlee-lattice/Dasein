#
# $LastChangedBy: YTian $
# $LastChangedDate: 2015-10-30 13:59:04 +0800 (Fri, 30 Oct 2015) $
# $Rev: 70693 $
#

import os, sys
import appsequence
import lp020200
import argparse


def parseArgs():
  parser = argparse.ArgumentParser()
  parser.add_argument('tenantFile', help="input tenant list csv file")
  parser.add_argument('results', help="specified output csv file")
  parser.add_argument("--op", choices=['upgrade', 'checkOnly'], help="upgrade or check the tenants")
  parser.add_argument("--buyerInsights", choices=['disable', 'daily', 'hourly'], help="disable/daily/hourly buyer insights")

  args = parser.parse_args()

  if args.op == 'upgrade':
    checkOnly = False

  tenantFileName = args.tenantFile

  resultFile = args.results

  buyerInsights = args.buyerInsights

  return (checkOnly, tenantFileName, resultFile, buyerInsights)


if __name__ == "__main__":
  PATCH_PATH = os.path.dirname(__file__)
  REVISION = '$Rev: 71901 $'

  print ''
  print 'PATH : {0}'.format(PATCH_PATH)
  print 'REV  : {0}'.format(REVISION)
  print ''

  buyerInsights = ''

  (checkOnly, tenantFileName, resultsFileName, buyerInsights) = parseArgs()

  sequence = []

  if buyerInsights == 'disable':
    sequence.append(lp020200.LP_020200_Disable_BuyerInsights())
  elif buyerInsights == 'daily':
    sequence.append(lp020200.LP_020200_Enable_Daily_BuyerInsights())
  elif buyerInsights == 'hourly':
    sequence.append(lp020200.LP_020200_Enable_Hourly_BuyerInsights())

  app = appsequence.AppSequence(tenantFileName, resultsFileName, sequence, checkOnly)
  app.execute()
