class PandasUtil():

    @staticmethod
    def insertIntoDataFrame(dataFrame, columnName, data, index = 0):
        columns = dataFrame.columns.tolist()
        columns.insert(index, columnName)
        dataFrame[columnName] = data
        return dataFrame[columns]

    @staticmethod
    def appendDataFrame(dataFrameTarget, dataFrameSource):
        for columnName in dataFrameSource.columns.tolist():
            data = dataFrameSource[columnName]
            index = len(dataFrameTarget.columns.tolist())
            dataFrameTarget = PandasUtil.insertIntoDataFrame(dataFrameTarget, columnName, data, index)
        return dataFrameTarget

    @staticmethod
    def shiftTail(dataFrame, shift, index = 0):
        columns = dataFrame.columns.tolist()
        popIndex = len(columns) - 1
        for _ in range(shift): columns.insert(index, columns.pop(popIndex))
        return dataFrame[columns]
