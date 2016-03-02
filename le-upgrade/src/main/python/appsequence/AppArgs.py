
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class AppArgs( object ):

  @classmethod
  def usage( cls, cmd, exit_code ):

    print 'Usage: {0} --checkOnly <tenant_list.csv> <results.csv>'.format( cmd )
    print 'Usage: {0} --upgrade <tenant_list.csv> <results.csv>'.format( cmd )
    print 'Usage: {0} --missingLead <tenantName>'.format( cmd )
    print ''
    
    exit( exit_code )


  @classmethod
  def get( cls, argv ):

    cmd = ''
    path = ''
    i = argv[0].rfind('\\')
    if( i != -1 ):
      path = argv[0][:i]
      cmd = argv[0][i+1:]

    if len(argv) == 1:
      cls.usage( cmd, 0 )

    # if len(argv) != 4 and len(argv) != 2:
    #   cls.usage( cmd, 1 )

    option = argv[1]

    if option not in ['--checkOnly','--upgrade','--missingLead']:
      cls.usage( argv[0], 1 )

    checkOnly = False
    if option == '--checkOnly':
      checkOnly = True

    if option in ['--checkOnly','--upgrade']:
      tenantFileName = argv[2]
      resultsFileName = argv[3]
      return (checkOnly, tenantFileName, resultsFileName)

    if option == '--missingLead':
      tenantName = argv[2]
      resultsFileName = argv[3]
      return  (tenantName,resultsFileName)
