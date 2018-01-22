from pyspark.sql import DataFrame


class StandardError():

    defaultCol = 'StandardError'

    def __init__(self, df = None):

        if df is None : raise TypeError

        self._df = df

        self._jStdErr = self._df._sc._jvm.com.ons.sml.api.java.methods.JavaStandardErrorFactory.standardError(self._df._jdf)

    def __mandatoryArgumentCheck(self, arg1, arg2, arg3):

        if (arg1 is None | arg2 is None | arg3 is None )  : raise TypeError('Missing mandatory argument')

    def stdErr1(self, df = None, xCol = None, yCol = None, zCol = None , outputCol = defaultCol):

        self.__mandatoryArgumentCheck(xCol, yCol, zCol)

        if df is None: df = self._df

        return DataFrame(self._jStdErr.stdErr1(df._jdf, xCol, yCol, zCol, outputCol) ,  df.sql_ctx)

def standardError(df) :

    return StandardError(df)

