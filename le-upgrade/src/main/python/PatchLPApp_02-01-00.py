
import os, sys
import appsequence

PATCH_PATH = os.path.dirname(__file__)
REVISION   = '$Rev$'

def Usage( cmd, exit_code ):
  
  
  print 'Usage: {0} --checkOnly <tenant_list.csv> <results.csv>'.format( cmd )
  print 'Usage: {0} --upgrade <tenant_list.csv> <results.csv>'.format( cmd )
  print ''
  
  exit( exit_code )


if __name__ == "__main__":

  cmd = ''
  path = ''
  i = sys.argv[0].rfind('\\')
  if( i != -1 ):
    path = sys.argv[0][:i]
    cmd = sys.argv[0][i+1:]

  print ''
  print 'PATH : {0}'.format( PATCH_PATH )
  print 'REV  : {0}'.format( REVISION )
  print ''

  if len(sys.argv) == 1:
    Usage( cmd, 0 )
  
  if len(sys.argv) != 4:
    Usage( cmd, 1 )

  option = sys.argv[1]

  if option not in ['--checkOnly','--upgrade']:
    Usage( sys.argv[0], 1 )

  checkOnly = False
  if option == '--checkOnly':
    checkOnly = True

  tenantFileName = sys.argv[2]
  resultsFileName = sys.argv[3]

  sequence = []
  sequence.append( appsequence.LPCheckVersion('2.0.1') )
  
  app = appsequence.AppSequence( tenantFileName, resultsFileName, sequence, checkOnly )
  app.execute()
