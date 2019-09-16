using System;
using System.Collections.Generic;

namespace CosmosMongoDBAggregationSample
{
    public class DateRange
    {
        public DateTime Begin { get; set; }
        public DateTime End { get; set; }
    }
    public class DateUtil
    {
       public static List<DateRange> GenerateRanges(
            DateTime begin,
            DateTime end,
            int intervalDays,
            int intervalHours,
            int intervalMins)
        {
            long intervalInMins = convertToMins(intervalDays, intervalHours, intervalMins);
            List<DateRange> dateRanges = new List<DateRange>();

            DateTime startTime = begin;
            while(startTime<end)
            {
                DateRange dr = new DateRange();
                dr.Begin = startTime;
                dr.End = startTime.AddMinutes(intervalInMins);

                startTime = dr.End;
                dateRanges.Add(dr);

            }
            return dateRanges;
        }

        private static long convertToMins(
            int intervalDays,
            int intervalHours,
            int intervalMins)
        {
            if (intervalDays < 0) { intervalDays = 0; }
            if (intervalHours < 0) { intervalHours = 0; }
            if (intervalMins < 0) { intervalMins = 0; }

            return intervalDays * 24 * 60 + intervalHours * 60 + intervalMins;
        }
    }
}
