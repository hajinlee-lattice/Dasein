from leframework.codestyle import overrides
from pipelinefwk import get_logger
from rulefwk import RowRule
import pandas as pd

logger = get_logger("AnonymousLeadRule")

class AnonymousLeadRule(RowRule):

    domainField = "Email"
    companyNameField = "CompanyName"
    rowsToRemove = {}
    idColumn = "LeadID"
    firstNameColumn = "FirstName"
    lastNameColumn = "LastName"
    threshold = 1

    def __init__(self, params, threshold=3):
        self.idColumn = params["idColumn"]

    @overrides(RowRule)
    def apply(self, dataFrame, configMetadata):
        rowsThatFailCondition = pd.DataFrame()

        self.rowsToRemove = {}
        if self.domainField in dataFrame.columns and self.companyNameField in dataFrame.columns:
            rowsThatFailCondition = dataFrame[pd.isnull(dataFrame[self.domainField]) & pd.isnull(dataFrame[self.companyNameField])]
            self.updateRowsToRemove([self.domainField, self.companyNameField], rowsThatFailCondition)

    def updateRowsToRemove(self, columnName, rowsThatFailCondition):
        if self.idColumn in rowsThatFailCondition.columns:
            for x in rowsThatFailCondition[self.idColumn]:
                self.rowsToRemove[x] = columnName

    @overrides(RowRule)
    def getRowsToRemove(self):
        return self.rowsToRemove

    @overrides(RowRule)
    def getDescription(self):
        return "A lead is anonymous if it is missing all domain and company location fields"
