from pyspark.sql import DataFrame


class Duplicate:
    """The Python Duplicate Marking Class"""

    defaultCol = "DuplicateMarking"

    def __init__(self, df=None):
        """

        :param df: The DataFrame passed into the duplicate class
        """

        if df is None:
            raise TypeError

        self._df = df

        self._jDup = self._df._sc._jvm.com.ons.sml.api.java.methods.JavaDuplicateFactory.duplicate(self._df._jdf)

    def __mandatoryArgumentCheck(self, arg1, arg2, arg3):
        """ this checks that the arguments are not none

        :param arg1:
        :param arg2:
        :param arg3:
        :return: An exception if any arguments are None
        """
        if (arg1 is None) or (arg2 is None) or (arg3 is None):
            raise TypeError

    def dm1(self, df=None, partCol=None, ordCol=None, new_col=None):
        """ The python implementation of the dm1 function

        :param df: A optional DataFrame that will be used inplace of the DataFrame given to the class
        :param partCol: A list of the column(s) to check for duplicates within
        :param ordCol: A list of the column(s) to order by. Later records are marked as duplicates
        :param new_col: The name of the new column that will contain the markers
        :return: DataFrame
        """

        self.__mandatoryArgumentCheck(partCol, ordCol, new_col)

        if df is None:
            df = self._df

        return DataFrame(
            self._jDup.dm1(
                df._jdf,
                partCol,
                ordCol,
                new_col
            ),
            df.sql_ctx
        )


def duplicate(df):
    """

    :param df:
    :return: A DataFrame
    """
    return Duplicate(df)
