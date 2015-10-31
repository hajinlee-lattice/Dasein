
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from .exceptions import UnknownMetadataValue

class QueryColumn(object):
 
    def __init__(self, name, expression, approved_usage = None, display_name = None,
                 category = None, statistical_type = None, tags = None,
                 fundamental_type = None, description = None,
                 display_discretization = None):

        self.initFromValues(name, expression, approved_usage, display_name, category,
                            statistical_type, tags, fundamental_type,
                            description, display_discretization)


    def getName(self):
        return self.name_

    def setName(self, n):
        self.name_ = n

    def getExpression(self):
        return self.exp_

    def setExpression(self, e):
        self.exp_ = e

    def getApprovedUsage(self):
        return self.approved_usage_

    def setApprovedUsage(self, au):
        if au is not None and au not in [ 'None', 'Model', 'ModelAndModelInsights', 'ModelAndAllInsights' ]:
            raise UnknownMetadataValue(au)
        self.approved_usage_ = au

    def getDisplayName(self):
        return self.display_name_

    def setDisplayName(self, dn):
        if dn is not None and len(dn) > 46:
            print 'Warning: truncating DisplayName \'{0}\' to 46 characters'.format(dn)
            self.display_name_ = dn[:46]
        else:
            self.display_name_ = dn

    def getCategory(self):
        return self.category_

    def setCategory(self, c):
        if c is not None and len(c) > 22:
            print 'Warning: truncating Category \'{0}\' to 22 characters'.format(c)
            self.category_ = c[:22]
        else:
            self.category_ = c

    def getStatisticalType(self):
        return self.statistical_type_

    def setStatisticalType(self, st):
        if st is not None and st not in [ 'nominal', 'ratio', 'ordinal', 'interval' ]:
            raise UnknownMetadataValue(st)
        self.statistical_type_ = st

    def getTags(self):
        return self.tags_

    def setTags(self, t):
        if t is not None and t.lower() not in [ 'internal', 'external' ]:
            raise UnknownMetadataValue(t)
        self.tags_ = t

    def getFundamentalType(self):
        return self.fundamental_type_

    def setFundamentalType(self, ft):
        if ft is not None and ft not in [ 'alpha', 'numeric', 'currency', 'percentage', 'boolean', 'year' ]:
            raise UnknownMetadataValue(ft)
        self.fundamental_type_ = ft

    def getDescription(self):
        return self.description_

    def setDescription(self, d):
        if d is not None and len(d) > 150:
            print 'Warning: truncating Description \'{0}\' to 150 characters'.format(d)
            self.description_ = d[:150]
        else:
            self.description_ = d

    def getDisplayDiscretization(self):
        return self.display_discretization_

    def setDisplayDiscretization(self, dd):
        self.display_discretization_ = dd

    def setMetadata(self, metadata_type, metadata_value):
        if metadata_type == 'ApprovedUsage':
            self.setApprovedUsage(metadata_value)
        elif metadata_type == 'DisplayName':
            self.setDisplayName(metadata_value)
        elif metadata_type == 'Category':
            self.setCategory(metadata_value)
        elif metadata_type == 'StatisticalType':
            self.setStatisticalType(metadata_value)
        elif metadata_type == 'Tags':
            self.setTags(metadata_value)
        elif metadata_type == 'FundamentalType':
            self.setFundamentalType(metadata_value)
        elif metadata_type == 'Description':
            self.setDescription(metadata_value)
        elif metadata_type == 'DisplayDiscretization':
            self.setDisplayDiscretization(metadata_value)

    def definition(self):
        raise NotImplementedError('QueryColumn.definition()')

    def initFromValues(self, name, expression, approved_usage = None, display_name = None,
                       category = None, statistical_type = None, tags = None,
                       fundamental_type = None, description = None,
                       display_discretization = None):

        self.setName(name)
        self.setExpression(expression)
        self.setApprovedUsage(approved_usage)
        self.setDisplayName(display_name)
        self.setCategory(category)
        self.setStatisticalType(statistical_type)
        self.setTags(tags)
        self.setFundamentalType(fundamental_type)
        self.setDescription(description)
        self.setDisplayDiscretization(display_discretization)
