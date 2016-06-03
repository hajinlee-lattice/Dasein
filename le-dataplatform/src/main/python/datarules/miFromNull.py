from leframework.codestyle import overrides
from data_profile import calculateMutualInfo
from rulefwk import ColumnRule

class miFromNull(ColumnRule):

    threshold = 50

    def __init__(self, threshold=50, eventColumn):
        if self.threshold > 0:
            self.threshold = threshold
        self.eventColumn = eventColumn

    @overrides
    def apply(self, dataFrame, dictOfArguments):
        mi, componentMi = calculateMutualInfo(dataFrame, self.eventVector)
        if None in componentMi:
            miFromNull = componentMi[None]
            if mi > 0:
                miFromNullAsPercentage = (miFromNull / mi) * 100
                return miFromNullAsPercentage > self.threshold
            else:
                return None
        else:
            return None

    @overrides
    def getRuleType(self):
        return "column"

    @overrides
    def explain(self):
        return "Check if MIFromNull is greater than " + self.threshold + "% of the MI of the column"
