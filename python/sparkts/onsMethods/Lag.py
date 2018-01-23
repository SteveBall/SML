from pyspark.sql import DataFrame

class Lag():

    def __init__(self, df = None):

        if df is None : raise TypeError

        self._df = df

        self._jLag = self._df._sc._jvm.com.ons.sml.api.java.methods.JavaLagFactory.lag(self._df._jdf)

    def lagFunc(self, df = None, partitionCols = None, orderCols = None, targetCol = None, lagNum = None):

        if df is None: df = self._df

        return DataFrame(self.jLag.lagFunc(df._jdf, partitionCols, orderCols, targetCol, lagNum), df.sql_ctx)

def lag(df) :

    return Lag(df)