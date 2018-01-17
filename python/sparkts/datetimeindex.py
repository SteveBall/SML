from py4j.java_gateway import java_import
from .utils import datetime_to_nanos
import numpy as np
import pandas as pd

class DateTimeIndex(object):
    """
    A DateTimeIndex maintains a bi-directional mapping between integers and an ordered collection of
    date-times. Multiple date-times may correspond to the same integer, implying multiple samples
    at the same date-time.

    To avoid confusion between the meaning of "index" as it appears in "DateTimeIndex" and "index"
    as a location in an array, in the context of this class, we use "location", or "loc", to refer
    to the latter.
    """ 

    def __init__(self, jdt_index):
        self._jdt_index = jdt_index

    def __len__(self):
        """Returns the number of timestamps included in the index."""
        return self._jdt_index.size()

    def _zdt_to_nanos(self, zdt):
        """Extracts nanoseconds from a ZonedDateTime"""
        instant = zdt.toInstant()
        return instant.getNano() + instant.getEpochSecond() * 1000000000

    def first(self):
        """Returns the earliest timestamp in the index, as a Pandas Timestamp."""
        return pd.Timestamp(self._zdt_to_nanos(self._jdt_index.first()))

    def last(self):
        """Returns the latest timestamp in the index, as a Pandas Timestamp."""
        return pd.Timestamp(self._zdt_to_nanos(self._jdt_index.last()))
    
    def datetime_at_loc(self, loc):
        """Returns the timestamp at the given integer location as a Pandas Timestamp."""
        return pd.Timestamp(self._zdt_to_nanos(self._jdt_index.dateTimeAtLoc(loc)))

    def __getitem__(self, val):
        # TODO: throw an error if the step size is defined
        if isinstance(val, slice):
            start = datetime_to_nanos(val.start)
            stop = datetime_to_nanos(val.stop)
            jdt_index = self._jdt_index.slice(start, stop)
            return DateTimeIndex(jdt_index)
        else:
            return self._jdt_index.locAtDateTime(datetime_to_nanos(val))

    def islice(self, start, end):
        """
        Returns a new DateTimeIndex, containing a subslice of the timestamps in this index,
        as specified by the given integer start and end locations.

        Parameters
        ----------
        start : int
            The location of the start of the range, inclusive.
        end : int
            The location of the end of the range, exclusive.
        """
        jdt_index = self._jdt_index.islice(start, end)
        return DateTimeIndex(jdt_index=jdt_index)

    def to_pandas_index(self):
        """Returns a pandas.DatetimeIndex representing the same date-times"""
        # TODO: we can probably speed this up for uniform indices
        arr = self._jdt_index.toNanosArray()
        return pd.DatetimeIndex(arr)

    def __eq__(self, other):
        return self._jdt_index.equals(other._jdt_index)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return self._jdt_index.toString()

class _Frequency(object):
    def __eq__(self, other):
        return self._jfreq.equals(other._jfreq)

    def __ne__(self, other):
       return not self.__eq__(other)

class DayFrequency(_Frequency):
    """
    A frequency that can be used for a uniform DateTimeIndex, where the period is given in days.
    """
  
    def __init__(self, days, sc):
        self._jfreq = sc._jvm.com.cloudera.sparkts.DayFrequency(days)

    def days(self):
        return self._jfreq.days()

class HourFrequency(_Frequency):
    """
    A frequency that can be used for a uniform DateTimeIndex, where the period is given in hours.
    """

    def __init__(self, hours, sc):
        self._jfreq = sc._jvm.com.cloudera.sparkts.HourFrequency(hours)

    def hours(self):
        return self._jfreq.hours()

class BusinessDayFrequency(object):
    """
    A frequency that can be used for a uniform DateTimeIndex, where the period is given in
    business days.  The first day of the business week is specified where Monday=1, Tuesday=2,
    and so on.
    """

    def __init__(self, bdays, firstDayOfWeek, sc):
        self._jfreq = sc._jvm.com.cloudera.sparkts.BusinessDayFrequency(bdays, firstDayOfWeek)

    def __eq__(self, other):
         return self._jfreq.equals(other._jfreq)

    def __ne__(self, other):
        return not self.__eq__(other)

    def days(self):
        return self._jfreq.days()

def uniform(start, end=None, periods=None, freq=None, sc=None):
    """
    Instantiates a uniform DateTimeIndex.

    Either end or periods must be specified.
    
    Parameters
    ----------
        start : string, long (nanos from epoch), or Pandas Timestamp
        end : string, long (nanos from epoch), or Pandas Timestamp
        periods : int
        freq : a frequency object
        sc : SparkContext
    """
    dtmodule = sc._jvm.com.cloudera.sparkts.__getattr__('DateTimeIndex$').__getattr__('MODULE$')
    if freq is None:
        raise ValueError("Missing frequency")
    elif end is None and periods == None:
        raise ValueError("Need an end date or number of periods")
    elif end is not None:
        return DateTimeIndex(dtmodule.uniformFromInterval( \
            datetime_to_nanos(start), datetime_to_nanos(end), freq._jfreq))
    else:
        return DateTimeIndex(dtmodule.uniform( \
            datetime_to_nanos(start), periods, freq._jfreq))

def irregular(timestamps, sc):
    """
    Instantiates an irregular DateTimeIndex.
    
    Parameters
    ----------
        timestamps : a Pandas DateTimeIndex, or an array of strings, longs (nanos from epoch), Pandas
            Timestamps
        sc : SparkContext
    """
    dtmodule = sc._jvm.com.cloudera.sparkts.__getattr__('DateTimeIndex$').__getattr__('MODULE$')
    arr = sc._gateway.new_array(sc._jvm.long, len(timestamps))
    for i in xrange(len(timestamps)):
        arr[i] = datetime_to_nanos(timestamps[i])
    return DateTimeIndex(dtmodule.irregular(arr))

class MillisecondFrequency(_Frequency):
    """
    A frequency that can be used for a uniform DateTimeIndex, where the period is given in milliseconds.
    """

    def __init__(self, milliseconds, sc):
        self._jfreq = sc._jvm.com.cloudera.sparkts.MillisecondFrequency(milliseconds)

    def milliseconds(self):
        return self._jfreq.milliseconds()


class MicrosecondFrequency(_Frequency):
    """
    A frequency that can be used for a uniform DateTimeIndex, where the period is given in microseconds.
    """

    def __init__(self, microseconds, sc):
        self._jfreq = sc._jvm.com.cloudera.sparkts.MicrosecondFrequency(microseconds)

    def microseconds(self):
        return self._jfreq.microseconds()

class SecondFrequency(_Frequency):
    """
    A frequency that can be used for a uniform DateTimeIndex, where the period is given in seconds.
    """

    def __init__(self, seconds, sc):
        self._jfreq = sc._jvm.com.cloudera.sparkts.SecondFrequency(seconds)

    def seconds(self):
        return self._jfreq.seconds()

class MinuteFrequency(_Frequency):
    """
    A frequency that can be used for a uniform DateTimeIndex, where the period is given in minutes.
    """

    def __init__(self, minutes, sc): 
        self._jfreq = sc._jvm.com.cloudera.sparkts.MinuteFrequency(minutes)

    def minutes(self):
        return self._jfreq.minutes()

class MonthFrequency(_Frequency):
    """
    A frequency that can be used for a uniform DateTimeIndex, where the period is given in months.
    """

    def __init__(self, months, sc):
        self._jfreq = sc._jvm.com.cloudera.sparkts.MonthFrequency(months)

    def months(self):
        return self._jfreq.months()

class YearFrequency(_Frequency):
    """
    A frequency that can be used for a uniform DateTimeIndex, where the period is given in years.
    """

    def __init__(self, years, sc):
        self._jfreq = sc._jvm.com.cloudera.sparkts.YearFrequency(years)

    def years(self):
        return self._jfreq.years()
