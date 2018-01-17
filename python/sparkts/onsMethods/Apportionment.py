from pyspark.sql import DataFrame


class Apportionment():

    defaultCol = 'AppResult'

    def __init__(self, df = None):

        if df is None : raise TypeError

        self._df = df

        self._jApp = self._df._sc._jvm.com.ons.sml.api.java.methods.JavaApportionmentFactory.apportionment(self._df._jdf)


    def __mandatoryArgumentCheck(self, arg1, arg2):

        if ((arg1 is None) or (arg2 is None)) : raise TypeError

    def app1(self, df = None, groupByCol = None, aggCol = None, appColName = defaultCol):

        self.__mandatoryArgumentCheck(groupByCol, aggCol)

        if df is None: df = self._df

        return DataFrame(self._jApp.app1(df._jdf, groupByCol, aggCol, appColName),  df.sql_ctx)

    def app2(self, df = None, groupByCol = None, aggCol = None, appColName = defaultCol):

        self.__mandatoryArgumentCheck(groupByCol, aggCol)

        if df is None: df = self._df

        return DataFrame(self._jApp.app2(df._jdf, groupByCol, aggCol, appColName),  df.sql_ctx)

    def app3(self, df = None, groupByCol = None, aggCol = None, appColName = defaultCol):

        self.__mandatoryArgumentCheck(groupByCol, aggCol)

        if df is None: df = self._df

        return DataFrame(self._jApp.app3(df._jdf, groupByCol, aggCol, appColName),  df.sql_ctx)

    def app4(self, df = None, groupByCol = None, aggCol = None, appColName = defaultCol):

        self.__mandatoryArgumentCheck(groupByCol, aggCol)

        if df is None: df = self._df

        return DataFrame(self._jApp.app4(df._jdf, groupByCol, aggCol, appColName),  df.sql_ctx)


def apportionment(df) :

    return Apportionment(df)