class ScoringUtil():

    @staticmethod
    def score(mediator, data, logger):
        
        scored = mediator.clf.predict_proba(data[mediator.schema["features"]])
        index = 1
        if len(scored) > 0 and len(scored[0]) < 2:
            logger.warn("All events have the same label.")
            index = 0
        return [sample[index] for sample in scored]
