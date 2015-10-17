
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class StepBase( object ):

  name        = 'Undefined'
  description = 'Undefined'
  version     = 'Undefined'

  def __init__( self, forceApply = False ):
    self._forceApply = forceApply

  def getName( self ):
    if self.name == 'Undefined':
      raise ValueError( 'Step name is required and is not defined' )
    return self.name

  def getDescription( self ):
    if self.description == 'Undefined':
      raise ValueError( 'Step description is required and is not defined' )
    return self.description

  def getVersion( self ):
    if self.version == 'Undefined':
      raise ValueError( 'Step version is required and is not defined' )
    return self.version

  def forceApply( self ):
    return self._forceApply

  def getApplicability( self, appseq ):
    raise ValueError( 'getApplicability() is required and is not defined' )

  def apply( self, appseq ):
    raise ValueError( 'apply() is required and is not defined' )
