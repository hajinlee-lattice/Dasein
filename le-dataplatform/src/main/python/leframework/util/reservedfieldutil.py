class ReservedFieldUtil():

    @staticmethod
    def configureReservedFields():
        configFields = [] # (Display Name, Default Value)
        configFields.append(("PercentileScore", int(-1)))
        configFields.append(("Score", float(-1)))
        configFields.append(("Converted", None))
        configFields.append(("Training", None))
        configFields.append(("PredictedRevenue", float(0)))
        configFields.append(("ExpectedRevenue", float(0)))

        # Decorate Names To Avoid Clashes
        lookup = dict()
        for (displayName, _) in configFields:
            lookup[displayName.lower()] = ReservedFieldUtil.decorateDisplayName(displayName)

        reservedFields = [lookup[displayName.lower()] for (displayName, _) in configFields]
        reservedFieldDefaultValues = [defaultValue for (_, defaultValue) in configFields]

        return (lookup, reservedFields, reservedFieldDefaultValues)

    @staticmethod
    def decorateDisplayName(displayName):
        return "###" + displayName + "###"

    @staticmethod
    def extractDisplayName(decoratedName):
        return decoratedName[3: -3]
