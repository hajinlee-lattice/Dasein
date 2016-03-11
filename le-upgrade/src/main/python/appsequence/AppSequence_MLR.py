
#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-12-24 17:00:02 +0800 (Thu, 24 Dec 2015) $
# $Rev: 71849 $
#

from liaison import *

from .Applicability import Applicability
from .StepBase      import StepBase
import traceback

class AppSequence_MLR( object ):

  def __init__( self, tenantName,resultsFileName, sequence,checkOnly):
  #  AppSequence.__init__(self, tenantName, resultsFileName, sequence, checkOnly)
    self._tenantFileName  = None
    self._resultsFileName = resultsFileName
    self._sequence        = sequence
    self._checkOnly       = False
    self._text            = {}
    self._tenants         = [tenantName]
    self._resultsFile     = None
    self._conn_mgr        = None
    self._lg_mgr          = None
    self._mode            = 'Upgrading'

  def execute( self ):
    self.beginJob()
    self.runSteps()
    self.endJob()


  def beginJob( self ):
    if self._tenants is None:
      raise ValueError( 'The input tenant name should not be None' )


    self._resultsFile = open( self._resultsFileName, mode = 'w' )
    self._resultsFile.write( 'TenantName,Upgraded' )
    for step in self._sequence:
      self._resultsFile.write( ',{0}:{1}'.format( step.getName(), step.getVersion() ) )
    self._resultsFile.write( '\n' )


  def endJob( self ):
    self._resultsFile.close()


  def runSteps( self ):

    for t in self._tenants:
      
      applicability      = {}
      checkNextStep      = True
      applyUpgrade       = True
      allStepsSuccessful = True

      print '{0} {1:25}:  '.format( self._mode, t ),

      try:
        self._conn_mgr = ConnectionMgrFactory.Create( 'visiDB', tenant_name=t )
        self._lg_mgr = self._conn_mgr.getLoadGroupMgr()
      except TenantNotMappedToURL:
        print 'Tenant \'{0}\' Not on LP DataLoader'.format( t )
        checkNextStep = False
        applyUpgrade = False
        continue
      
      sequence_applicable = []

      print 'Checking Applicability',

      for step in self._sequence:

        if not checkNextStep:
          applicability[step.getName()] = 'Not Checked'
          continue

        thisStep = Applicability.canApply
        if not step.forceApply():
          thisStep = step.getApplicability( self )

        if thisStep == Applicability.canApply:
          applicability[step.getName()] = 'To Apply'
          sequence_applicable.append( step )
        elif thisStep == Applicability.alreadyAppliedPass:
          applicability[step.getName()] = 'Previously Applied'
        elif thisStep == Applicability.alreadyAppliedFail:
          applicability[step.getName()] = 'Previously Applied; Failed'
          checkNextStep = False
          applyUpgrade = False
        elif thisStep == Applicability.cannotApplyPass:
          applicability[step.getName()] = 'Cannot Apply; Ignored'
        elif thisStep == Applicability.cannotApplyFail:
          applicability[step.getName()] = 'Cannot Apply; Failed'
          checkNextStep = False
          applyUpgrade = False

        print '.',

      if not self._checkOnly and applyUpgrade:

        print 'Applying Upgrade',
        try:
          for step in sequence_applicable:
            success = step.apply( self )
            if not success:
              applicability[step.getName()] = 'UNEXPECTED FAILURE'
              allStepsSuccessful = False
              break
            print '.',
          self._lg_mgr.commit()
          print 'Done'
        except Exception, exception:
          print 'Unexpected Failure during upgrade'
          print "\nException arguments:", exception.args
          print "\nException message:", exception
          print "\nTraceback:"
          traceback.print_exc()
      else:
        allStepsSuccessful = False
        print 'Continuing'


      self._resultsFile.write( '{0},{1}'.format( t, allStepsSuccessful ) )

      for step in self._sequence:
        thisStep = applicability[step.getName()]
        if allStepsSuccessful and applicability[step.getName()] == 'To Apply':
          thisStep = 'Applied'
        self._resultsFile.write( ',{0}'.format( thisStep ) )

      self._resultsFile.write( '\n' )


  def setText( self, name, value ):
    self._text[name] = value


  def getText( self, name ):
    if name in self._text:
      return self._text[name]
    return 'Undefined'


  def setConnectionMgr( self, conn_mgr ):
    self._conn_mgr = conn_mgr


  def getConnectionMgr( self ):
    return self._conn_mgr


  def setLoadGroupMgr( self, lg_mgr ):
    self._lg_mgr = lg_mgr


  def getLoadGroupMgr( self ):
    return self._lg_mgr
