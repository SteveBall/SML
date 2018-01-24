from pyspark.sql import DataFrame


class Lag:
    """
    This class contains method which perform the Lag Function
    """

    def __init__(self, df = None):
        """
        Initialise function to instantiate the Lag class

        :param df: Input DataFrame
        """

        if df is None : raise TypeError

        self._df = df

        self._jLag = self._df._sc._jvm.com.ons.sml.api.java.methods.JavaLagFactory.lag(self._df._jdf)

    def __mandatoryArgumentCheck(self, arg1, arg2, arg3):
        if (arg1 is None) or (arg2 is None) or (arg3 is None):
            raise TypeError

    def lagFunc(self, df = None, partitionCols = None, orderCols = None, targetCol = None, lagNum = None):
        """
        Method: Lag Function
        Version 1.0
        author: Ian Edward

        :param df: Input DataFrame
        :param partitionCols: Columns(s) name to partition on
        :param orderCols:  Column(s) name to order on
        :param targetCol: Column(s) name to create a window on
        :param lagNum: The number of rows back from the current row from which to obtain a value.
        :return: pyspark.sql.DataFrame
        """

        self.__mandatoryArgumentCheck(partitionCols, orderCols, targetCol)

        if df is None:
            df = self._df

        return DataFrame(
            self._jLag.lagFunc(
                df._jdf,
                partitionCols,
                orderCols,
                targetCol,
                lagNum
            ),
            df.sql_ctx
        )


def lag(df):

    return Lag(df)
