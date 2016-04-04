
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class HTTPError( ValueError ):
    """An error was returned from an HTTP request"""

class EndpointError( ValueError ):
    """An error was returned from an HTTP request"""

class ExpressionSyntaxError( ValueError ):
    """The given expression has a syntax error"""

class ExpressionNotImplemented( ValueError ):
    """The given expression has not been implemented yet"""

class TenantNotMappedToURL( ValueError ):
    """There is no entry in the tenant-url map for this tenant"""

class TenantNotFoundAtURL( ValueError ):
    """The tenant was not found at the specified URL"""

class VisiDBNotFoundOnServer( ValueError ):
    """The tenant was not found at the specified URL"""

class DataLoaderError( ValueError ):
    """The tenant was not found at the specified URL"""

class UnknownMetadataValue( ValueError ):
    """The given metadata value is unknown"""

class UnknownVisiDBSpec( ValueError ):
    """A requested visiDB spec is unknown"""

class UnknownDataLoaderObject( ValueError ):
    """A requested DataLoader object is unknown"""

class UnknownDatabaseType( ValueError ):
    """The given database technology is unknown in this module"""

class MaudeStringError( ValueError ):
    """The given Maude string has a syntax error"""

class XMLStringError( ValueError ):
    """The given xml string has a syntax error"""

class UnknownVisiDBType( ValueError ):
    """The given type is not a valid type in visiDB"""

class VisiDBQueryResultsNotReady( ValueError ):
    """The results for a visiDB query are not yet ready"""
