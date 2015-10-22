
#
# $LastChangedBy$
# $LastChangedDate: 2015-10-20 23:33:11 -0700 (Tue, 20 Oct 2015) $
# $Rev$
#

import sys,re
import appsequence,liaison


class SpecData( object ):
  def __init__( self, vdbtype, defn, slne ):
    self.vdbtype = vdbtype
    self.defn = defn
    self.slne = slne


def ReportTemplateDifferences( tenant_1, tenant_2, template_type, descriptor ):

  vDBResultsFileName = template_type + '_Upgraded_visiDB_Objects_' + descriptor + '.csv'
  DLResultsFileName = template_type + '_Upgraded_DataLoader_Objects_' + descriptor + '.csv'
  newSpecsFileName = template_type + '_NewSpecs_' + descriptor + '.maude'
  newLGsFileName = template_type + '_NewLGs_' + descriptor + '.config'

  specmap_1 = GetSpecsDictionary( tenant_1 )
  specmap_2 = GetSpecsDictionary( tenant_2 )

  with open( vDBResultsFileName, mode='w' ) as vDBResultsFile:
    with open( newSpecsFileName, mode='w' ) as newSpecsFile:

      vDBResultsFile.write('ContainerElementName,Operator,New or Modified\n')
      newSpecsFile.write('SpecLatticeNamedElements((\n')

      for (name,specdata) in specmap_2.iteritems():

        if name not in specmap_1:
          vDBResultsFile.write( '{0},{1},New\n'.format(name,specdata.vdbtype) )
          newSpecsFile.write( '  {0}\n'.format(specdata.slne) )
        elif specmap_1[name].defn != specdata.defn:
          vDBResultsFile.write( '{0},{1},Modified\n'.format(name,specdata.vdbtype) )

      newSpecsFile.write('))\n')


def GetSpecsDictionary( tenant ):

  try:
    conn_mgr = liaison.ConnectionMgrFactory.Create( 'visiDB', tenant_name=tenant, verify=False )
  except liaison.TenantNotMappedToURL:
    print 'Tenant \'{0}\' is not mapped to a DataLoader URL'.format( tenant )
    exit(10)

  slne = conn_mgr.GetAllSpecs()

  s1 = re.search( '^SpecLatticeNamedElements\((.*)\)$', slne )
  if not s1:
    print 'Tenant \'{0}\' has unrecognizable SpecLatticeNamedElements()'.format( tenant )
    exit(20)

  specs_maude = s1.group(1)
  specs = {}

  if specs_maude != 'empty':

    specs_maude = specs_maude[1:-1]

    while True:

      s2 = re.search( '^(SpecLatticeNamedElement.*?)(, SpecLatticeNamedElement.*|$)', specs_maude )
      if not s2:
        print 'Tenant \'{0}\' has unrecognizable SpecLatticeNamedElement()'.format( tenant )
        exit(30)

      singlespec    = s2.group(1)
      remainingspec = s2.group(2)

      s3 = re.search( 'SpecLatticeNamedElement\(((SpecLattice.*?)\(.*\)), ContainerElementName\(\"(.*?)\"\)\)$', singlespec )
      if not s3:
        print 'Cannot parse spec for Tenant \'{0}\':\n\n{1}\n'.format( tenant, singlespec )
        exit(40)

      defn    = s3.group(1)
      vdbtype = s3.group(2)
      name    = s3.group(3)

      if vdbtype not in set(['SpecLatticeExtract','SpecLatticeBinder']):

        if name not in specs:
          specs[name] = SpecData( vdbtype, defn, singlespec )

      if remainingspec == '':
        break
      else:
        specs_maude = remainingspec[2:]

  return specs


def Usage( cmd, exit_code ):
  
  print 'Usage: {0} <tenant_1> <tenant_2> <template_type> <descriptor>'.format( cmd )
  print ''
  print 'Creates four files:'
  print ' <template_type>_Upgraded_visiDB_Objects_<descriptor>.csv'
  print ' <template_type>_Upgraded_DataLoader_Objects_<descriptor>.csv'
  print ' <template_type>_NewSpecs_<descriptor>.maude'
  print ' <template_type>_NewLGs_<descriptor>.config'
  print ''
  
  exit( exit_code )


if __name__ == "__main__":

  cmd = ''
  path = ''
  i = sys.argv[0].rfind('\\')
  if( i != -1 ):
    path = sys.argv[0][:i]
    cmd = sys.argv[0][i+1:]

  if len(sys.argv) == 1:
    Usage( cmd, 0 )
  
  if len(sys.argv) != 5:
    Usage( cmd, 1 )

  tenant_1           = sys.argv[1]
  tenant_2           = sys.argv[2]
  template_type      = sys.argv[3]
  descriptor         = sys.argv[4]

  ReportTemplateDifferences( tenant_1, tenant_2, template_type, descriptor )
