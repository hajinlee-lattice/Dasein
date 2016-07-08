from leframework.codestyle import overrides
from pipelinefwk import get_logger
from rulefwk import RowRule

logger = get_logger("LeadPerDomainRule")

class LeadPerDomainRule(RowRule):

    rowsToRemove = {}
    emailField = "Email"
    domainFields = ["Email", "Domain", "Website"]
    idColumn = "LeadID"

    def __init__(self, params):
        self.idColumn = params["idColumn"]

    @overrides(RowRule)
    def apply(self, dataFrame, configMetadata):
        self.rowsToRemove = {}

        for field in self.domainFields:
            if field in dataFrame.columns:
                groupedDataFrame = dataFrame.groupby(field).nth(1).dropna()

                setIntersection = set(set(dataFrame[self.idColumn].values).intersection(set(groupedDataFrame[self.idColumn].values)))
                setDifference = set(set(dataFrame[self.idColumn].values).difference(set(groupedDataFrame[self.idColumn].values)))

            if len(setIntersection) > 0:
                self.rowsToRemove = dict((x, [field]) for x in setDifference)

    @overrides(RowRule)
    def getRowsToRemove(self):
        return self.rowsToRemove

    @overrides(RowRule)
    def getDescription(self):
        return "Check if rows have unique domain(email, domain or website). If more than one row has the same domain, mark one row \
            for inclusion and all other rows for removal. "
