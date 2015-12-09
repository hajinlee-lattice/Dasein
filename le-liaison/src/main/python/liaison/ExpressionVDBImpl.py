
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re
from tokenize import generate_tokens, TokenError
from StringIO import StringIO
import token
from .exceptions import ExpressionNotImplemented, MaudeStringError, UnknownVisiDBType
from . import types_vdb


class ExpressionVDBImplFactory( object ):

    @classmethod
    def Create( cls, defn ):

        if re.search( '^LatticeFunctionExpressionConstant', defn ):
            if re.search( '^LatticeFunctionExpressionConstantNull', defn ):
                return ExpressionVDBImplGeneric.InitFromDefn(defn)
            return ExpressionVDBImplConst.InitFromDefn( defn )

        elif re.search( '^LatticeFunctionIdentifier\(ContainerElementName\(', defn ):
            return ExpressionVDBImplFcnRef.InitFromDefn( defn )
        
        elif re.search( '^LatticeFunctionIdentifier\(ContainerElementNameTableQualifiedName\(', defn ):
            return ExpressionVDBImplColRef.InitFromDefn( defn )

        return ExpressionVDBImplGeneric.InitFromDefn(defn)


    @classmethod
    def Parse( cls, str ):

        g = generate_tokens( StringIO(str).readline )
        tok_iter = iter(g)
        return cls.GenerateExpression( str, tok_iter )


    @classmethod
    def GenerateExpression( cls, str, tok_iter, tok_endmarker = None ):
        args = []
        try:
            while(True):
                (toknum, tokval, _, _, _) = tok_iter.next()
                
                if ( tok_endmarker is not None and tokval == tok_endmarker ) or ( toknum == token.ENDMARKER ):
                    if len(args) == 1:
                        break
                    else:
                        raise ExpressionSyntaxError( str )
                
                elif tokval == '(':
                    args.append( cls.GenerateExpression( str, tok_iter, ')' ) )
                
                elif toknum == token.OP and tokval == '.':
                    tablename = args.pop().FcnName()
                    (toknum, colname, _, _, _) = tok_iter.next()
                    if toknum != token.NAME:
                        raise ExpressionSyntaxError( str )
                    args.append( ExpressionVDBImplColRef(colname,tablename) )
                
                elif toknum == token.NAME:
                    args.append( ExpressionVDBImplFcnRef.InitFromTokens(tokval) )
                
                elif toknum == token.OP:
                    raise ExpressionNotImplemented( 'Operator not implemented: {0}'.format( tokval ) )
                
                else:
                    args.append( ExpressionVDBImplConst.InitFromTokens(toknum,tokval) )

        except StopIteration:
            raise ExpressionSyntaxError( str )

        except TokenError:
            raise ExpressionSyntaxError( str )

        return args.pop()


    


class ExpressionVDBImpl( object ):

    def definition( self ):
        return ''


class ExpressionVDBImplGeneric( ExpressionVDBImpl ):

    def __init__( self, spec_str ):

        self.InitFromValues( spec_str )

    @classmethod
    def InitFromDefn( cls, defn ):

        return cls( defn )

    def definition( self ):

        return self._spec_str

    def InitFromValues( self, spec_str ):

        self._spec_str = spec_str


class ExpressionVDBImplConst( ExpressionVDBImpl ):

    def __init__( self, value, datatype, isScalar = False ):

        self.InitFromValues( value, datatype, isScalar )

    @classmethod
    def InitFromDefn( cls, defn ):

        c = re.search( '^LatticeFunctionExpressionConstant((Scalar)?)\("(.*)", DataType(.*)\)$', defn )

        if c:
            isScalar = False
            if c.group(2) is not None:
                isScalar = True
            return cls( c.group(3), c.group(4), isScalar )
        else:
            raise MaudeStringError( defn )

    @classmethod
    def InitFromTokens( cls, toknum, tokval ):

        if toknum == token.NUMBER:
            if '.' in tokval:
                datatype = 'Double'
            else:
                datatype = 'Long'
        elif toknum == token.STRING:
            tokval = tokval[1:len(tokval)-1]
            datatype = 'VarChar({0})'.format( len(tokval) )
        else:
            ## Bit, Datetime
            raise ExpressionNotImplemented( 'Constant not implemented: {0}'.format( token.tok_name[toknum] ) )

        return cls( tokval, datatype )

    def Value( self ):
        return self._value

    def SetValue( self, v ):
        self._value = v
        return self._value

    def Datatype( self ):
        return self._datatype

    def SetDatatype( self, dt ):
        self._datatype = dt
        return self._datatype

    def definition( self ):

        type = 'LatticeFunctionExpressionConstant'
        if self._isScalar:
            type = 'LatticeFunctionExpressionConstantScalar'
        return '{0}(\"{1}\", DataType{2})'.format( type, self.Value(), self.Datatype() )

    def InitFromValues( self, value, datatype, isScalar = False ):

        if not types_vdb.IsStandardType( datatype ):
            raise UnknownVisiDBType( datatype )

        self._value = value
        self._datatype = datatype
        self._isScalar = isScalar


class ExpressionVDBImplFcnRef( ExpressionVDBImpl ):

    def __init__( self, fcn ):

        self.InitFromValues( fcn )

    @classmethod
    def InitFromDefn( cls, defn ):

        c = re.search( '^LatticeFunctionIdentifier\(ContainerElementName\("(.*)"\)\)$', defn )

        if c:
            return cls( c.group(1) )
        else:
            raise MaudeStringError( defn )

    @classmethod
    def InitFromTokens( cls, tokval ):

        return cls( tokval )

    def FcnName( self ):
        return self._fcn

    def SetFcnName( self, f ):
        self._fcn = f
        return self._fcn

    def definition( self ):

        return 'LatticeFunctionIdentifier(ContainerElementName("{0}"))'.format( self.FcnName() )

    def InitFromValues( self, fcn ):

        self._fcn = fcn


class ExpressionVDBImplColRef( ExpressionVDBImpl ):

    def __init__( self, colname, tablename ):

        self.InitFromValues( colname, tablename )

    @classmethod
    def InitFromDefn( cls, defn ):

        c = re.search( '^LatticeFunctionIdentifier\(ContainerElementNameTableQualifiedName\(LatticeSourceTableIdentifier\(ContainerElementName\("(.*)"\)\), ContainerElementName\("(.*)"\)\)\)$', defn )

        if c:
            return cls( c.group(2), c.group(1) )
        else:
            raise MaudeStringError( defn )

    @classmethod
    def InitFromTokens( cls, tokval ):

        pass

    def ColumnName( self ):
        return self._colname

    def SetColumnName( self, c ):
        self._colname = c
        return self._colname

    def TableName( self ):
        return self._tablename

    def SetTableName( self, t ):
        self._tablename = t
        return self._tablename

    def definition( self ):

        return 'LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("{0}")), ContainerElementName("{1}")))'.format( self.TableName(), self.ColumnName() )

    def InitFromValues( self, colname, tablename ):

        self._colname = colname
        self._tablename = tablename
