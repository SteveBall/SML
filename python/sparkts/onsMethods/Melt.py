from pyspark.sql import DataFrame

class Melt():

    def __init__(self, df = None):

        if df is None : raise TypeError

        self._df = df

        self._jMelt = self._df._sc._jvm.com.ons.sml.api.java.methods.JavaMeltFactory.melt(self._df._jdf)

    def melt1(self, df = None, id_vars = None, value_vars = None, var_name = None, val_name = None ):

        if df is None: df = self._df

        return DataFrame(self._jMelt.app1(df._jdf, id_vars, value_vars, var_name, val_name),  df.sql_ctx)