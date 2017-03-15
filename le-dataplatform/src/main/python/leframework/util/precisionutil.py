import numpy as np

class PrecisionUtil(object):

    __standardPrecision = 8

    @classmethod
    def setPlatformStandardPrecision(cls, x):
        return cls.setPrecision(x, cls.__standardPrecision)

    @classmethod
    def setPrecisionOfValue(cls, x, precision):
        if x is np.isnan(x):
            return x
        if type(x) == int:
            return x
        if x == 0.0:
            return x
        l = np.log10(np.fabs(x))
        e = np.int64(l)
        n = precision if l < 0.0 else precision - 1
        m = round(float(x) / 10.0**e, n)
        return m*10.0**e

    @classmethod
    def setPrecisionOfNPArray(cls, a, precision):
        if a is None:
            return None
        if a.dtype == np.int64:
            return np.array(a)
        return np.array([cls.setPrecisionOfValue(v, precision) for k,v in a.iteritems()])

    @classmethod
    def setPrecision(cls, x, precision):
        if x is None:
            return None
        xprec = x
        try:
            xprec = cls.setPrecisionOfNPArray(x, precision)
        except AttributeError:
            xprec = cls.setPrecisionOfValue(x, precision)
        except Exception as e:
            pass
        return xprec
