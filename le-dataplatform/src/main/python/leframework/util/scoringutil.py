import random

class ScoringUtil():

    @staticmethod
    def score(mediator, data, logger):
        
        if mediator.clf is None:
            return []
        scored = mediator.clf.predict_proba(data[mediator.schema["features"]])
        index = 1
        if len(scored) > 0 and len(scored[0]) < 2:
            logger.warn("All events have the same label.")
            index = 0
        return [sample[index] for sample in scored]
    
    @staticmethod
    def sortWithRandom(df, axis=0, seed=-1, scale=.0001):
        random.seed(seed)
        length = df.shape[0]
        values = df.iloc[:, axis].tolist()
        mean = float(sum(values)) / length
        shift = [scale * mean * random.uniform(0, 1) for _ in range(length)]
        ind = sorted(range(len(values)), key=lambda i: values[i] + shift[i], reverse=True)
        return df.iloc[ind]
