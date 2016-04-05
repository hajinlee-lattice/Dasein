#!/usr/bin/python

#
# $LastChangedBy: lyan $
# $LastChangedDate: 2016-02-01 17:36:38 +0800 (Mon, 01 Feb 2016) $
# $Rev: 72469 $
#

import os, sys
import appsequence
import lp020401

PATCH_PATH = os.path.dirname(__file__)
REVISION   = '$Rev: 72469 $'

print ''
print 'PATH : {0}'.format( PATCH_PATH )
print 'REV  : {0}'.format( REVISION )
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)

sequence = []
sequence.append( appsequence.LPCheckVersion('2.2.0', True) )
sequence.append( lp020401.LP_020401_GetDistinctGUID() )
#sequence.append( appsequence.LPSetVersion('2.4.1') )

app = appsequence.AppSequence( tenantFileName, resultsFileName, sequence, checkOnly )
app.execute()
