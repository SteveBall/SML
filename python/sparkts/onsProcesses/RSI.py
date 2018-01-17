from pyspark.sql import DataFrame

class RSI():


    def __init__(self, df = None):

        if df is None : raise TypeError

        self._df = df
        self._jRSI = df._sc._jvm.com.ons.sml.api.java.processes.JavaRSIFactory.rsi(self._df._jdf)

    def runProcess(self, runId):

        return DataFrame(self._jRSI.runProcess(runId), self._df.sql_ctx)


def rsi(df) :

    return RSI(df)