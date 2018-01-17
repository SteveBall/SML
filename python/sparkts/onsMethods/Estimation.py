from pyspark.sql import DataFrame


class Estimation():

    defaultCol = 'EstResult'

    def __init__(self, df = None):

        if df is None : raise TypeError

        self._df = df

        self._jEst = self._df._sc._jvm.com.ons.sml.api.java.methods.JavaEstimationFactory.estimation(self._df._jdf)

    def __mandatoryArgumentCheck(self, arg1):

        if arg1 is None : raise TypeError


    def est1(self, df = None, inputCol = None, factor = 1, outputCol = defaultCol):

        self.__mandatoryArgumentCheck(inputCol)

        if df is None: df = self._df

        return DataFrame(self._jEst.est1(df._jdf, inputCol, factor, outputCol),  df.sql_ctx)

    def est2(self, df = None, inputCol = None, factor = 1, outputCol = defaultCol):

        self.__mandatoryArgumentCheck(inputCol)

        if df is None: df = self._df

        if factor == 0 : raise Exception('Factor cannot be zero')

        return DataFrame(self._jEst.est2(df._jdf, inputCol, factor, outputCol),  df.sql_ctx)




def estimation(df) :

    return Estimation(df)

