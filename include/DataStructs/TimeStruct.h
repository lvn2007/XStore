#ifndef XSTORE_TIMESTRUCTS_H
#define XSTORE_TIMESTRUCTS_H

#include <cstdint>
#include <ctime>
#include "XStore.pb.h"

// Data structure to hold time-value together
struct TimeStruct {
  std::time_t UnixTime;
  XStore_Proto::TimeUnit TimeResolution;
  unsigned short int Year, Month, Day, Hour, Minute, Second;
  unsigned short int Weekday; // 0 = Sunday .. 6 = Saturday (matches tm_wday)
  int64_t FirstEpoch, LastEpoch;

  // Constructor
  TimeStruct(std::time_t unixTime, XStore_Proto::TimeUnit timeResolution);

  // Returns last day of a given month provided the year given in unixTime
  unsigned short int GetLastDayOfMonth(unsigned short int iMonth) const;

  private:
    void ExtractTimeComponents();
    bool IsLeapYear() const;
};

#endif //XSTORE_TIMESTRUCTS_H
