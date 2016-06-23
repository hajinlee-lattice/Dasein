from leframework.codestyle import overrides
from pipelinefwk import get_logger
from rulefwk import RowRule
from dataruleutils import selectIdColumn
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

    def __init__(self, threshold=3):
        pass

    @overrides(RowRule)
    def apply(self, dataFrame, configMetadata):
        rowsThatFailCondition = pd.DataFrame()

        self.selectIDColumn(dataFrame)

        self.rowsToRemove = {}
        if self.domainField in dataFrame.columns and self.companyNameField in dataFrame.columns:
            rowsThatFailCondition = dataFrame[pd.isnull(dataFrame[self.domainField]) & pd.isnull(dataFrame[self.companyNameField])]
            self.updateRowsToRemove([self.domainField, self.companyNameField], rowsThatFailCondition)

    def selectIDColumn(self, dataFrame):
        self.idColumn = selectIdColumn(dataFrame)

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
