
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re
from .exceptions import ExpressionSyntaxError, ExpressionNotImplemented, MaudeStringError, UnknownVisiDBType
from . import types_vdb
from .NamedExpression import NamedExpression
from .ExpressionVDBImpl import *


class NamedExpressionVDBImpl( NamedExpression ):

## This is the default constructor

    def __init__( self, name, exp, other_specs = None ):

        self.InitFromValues( name, exp, other_specs )


## This is the pythonic way to create multiple constructors.  The call is
## e = NamedExpressionVDBImpl.InitFromDefn( defn )

    @classmethod
    def InitFromDefn( cls, name, defn ):
        
        s = re.search( '^SpecLatticeFunction\((.*), (DataType(.*?), SpecFunctionType(.*?), SpecFunctionSourceType(.*?), SpecDefaultValue(.*?), SpecDescription\("(.*?)"\))\)$', defn )
        if not s:
            raise MaudeStringError( defn )

        defn = s.group(1)
        other_specs = s.group(2)

        return cls( name, ExpressionVDBImplFactory.Create(defn), other_specs )


    @classmethod
    def Parse( cls, name, str ):

        return cls( name, ExpressionVDBImplFactory.Parse(str) )


    def Object( self ):
        return self._exp


    def SetObject( self, obj ):
        self._exp = obj
        return self._exp


    def OtherSpecs( self ):
        return self._other_specs


    def SetOtherSpecs( self, os ):
        self._other_specs = os
        return self._other_specs


    def Definition( self ):

        defn =  'SpecLatticeFunction('
        defn +=   '{0}'.format( self._exp.Definition() )
        defn += ', {0}'.format( self.OtherSpecs() )
        defn += ')'

        return defn


    def SpecLatticeNamedElements( self ):

        defn =  'SpecLatticeNamedElements(('
        defn +=   'SpecLatticeNamedElement('
        defn += self.Definition()
        defn +=   ', ContainerElementName(\"{0}\")'.format( self.Name() )
        defn +=   ')'
        defn += '))'

        return defn


    def InitFromValues( self, name, exp, other_specs = None ):

        super( NamedExpressionVDBImpl, self ).InitFromValues( name )

        if other_specs is not None:
            self._other_specs = other_specs

        else:
            self._other_specs =    'DataTypeUnknown'
            self._other_specs += ', SpecFunctionTypeMetric'
            self._other_specs += ', SpecFunctionSourceTypeCalculation'
            self._other_specs += ', SpecDefaultValueNull'
            self._other_specs += ', SpecDescription("")'

        self._exp = exp
