from pyspark.sql import DataFrame

class VAT():


    def __init__(self, df = None):

        if df is None : raise TypeError

        self._df = df
        self._jVAT = df._sc._jvm.com.ons.sml.api.java.processes.JavaVATFactory.vat(self._df._jdf)

    def runProcess(self, runId):

        return DataFrame(self._jVAT.runProcess(runId), self._df.sql_ctx)


def vat(df) :

    return VAT(df)