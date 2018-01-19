from pyspark.sql import DataFrame


class Duplicate:

    defaultCol = "DuplicateMarking"

    def __init__(self, df=None):

        if df is None:
            raise TypeError

        self._df = df

        self._jDup = self._df._sc._jvm\
            .com.ons.sml.api.java.methods.JavaDuplicateFactory.duplicate(self._df._jdf)

    def __mandatoryArgumentCheck(self, arg1, arg2, arg3):
        if (arg1 is None) or (arg2 is None) or (arg3 is None):
            raise TypeError

    def dm1(self, df=None, partCol=None, ordCol=None, new_col=None):

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
    return Duplicate(df)
