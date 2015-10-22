
import os, sys
import appsequence
import lp020100

PATCH_PATH = os.path.dirname(__file__)
REVISION   = '$Rev$'

print ''
print 'PATH : {0}'.format( PATCH_PATH )
print 'REV  : {0}'.format( REVISION )
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)

sequence = []
sequence.append( appsequence.LPCheckVersion('2.0') )
sequence.append( lp020100.LP_020100_NewSpecs() )
sequence.append( lp020100.LP_020100_AddDataProvider() )

app = appsequence.AppSequence( tenantFileName, resultsFileName, sequence, checkOnly )
app.execute()
