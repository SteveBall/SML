from pyspark.sql import DataFrame


class StandardError():
    """

    This class is to calculate the standard error for input data based on formula:
    {{{ stdError = sqrt{ (x/y)*(x-y) / (y-1) } * z  }}} and x,y,z are columns in the input dataframe.

    It accepts the input dataframe, applies the above calculation and output the dataframe with a new column
    along calculated 'standardError' value.

    """
    defaultCol = 'StandardError'

    def __init__(self, df=None):
        """
        Instantiates a JavaStandardFactory from a DataFrame.

        :param df: Input dataframe.
        """

        if df is None:
            raise TypeError

        self._df = df

        self._jStdErr = self._df._sc._jvm.com.ons.sml.api.java.methods.JavaStandardErrorFactory.standardError(
            self._df._jdf)

    def __mandatoryArgumentCheck(self, arg1, arg2, arg3):
        """
        This function raise Type Error if any one of the input arg is None.

        :param arg1: Input argument 1
        :param arg2: Input argument 2
        :param arg3: Input argument 3
        """
        if (arg1 is None) | (arg2 is None) | (arg3 is None):
            raise TypeError('Missing mandatory argument')

    def stdErr1(self, df, xCol, yCol, zCol, outputCol=defaultCol):
        """

        This function will add an extra column on to a DataFrame containing the standard error
        This is calculated using the formula:
                {{{ stdError = sqrt{ (x/y)*(x-y) / (y-1) } * z  }}}

        :param df: Input dataframe
        :param xCol: The column to be used as X in the calculation
        :param yCol: The column to be used as Y in the calculation
        :param zCol: The column to be used as Z in the calculation
        :param outputCol: The name of the standard error column
        :return: Dataframe with a extra column for standardError
        """
        self.__mandatoryArgumentCheck(xCol, yCol, zCol)

        if df is None:
            df = self._df

        return DataFrame(self._jStdErr.stdErr1(df._jdf, xCol, yCol, zCol, outputCol), df.sql_ctx)

def standardError(df):
    """
    This function instantiates a StandardError by passing the input dataframe.

    :param df: The input dataframe.
    :return: Dataframe with a extra column for standardError
    """
    return StandardError(df)
