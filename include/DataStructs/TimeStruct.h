#ifndef XSTORE_TIMESTRUCTS_H
#define XSTORE_TIMESTRUCTS_H

#include <cstdint>
#include <ctime>
#include "XStore.pb.h"

// Data structure to hold time-value together
struct TimeStruct {
  std::time_t unix_time;
  XStore_Proto::TimeUnit time_resolution;
  unsigned short int year, month, day, hour, minute, second;
  unsigned short int weekday; // 0 = Sunday .. 6 = Saturday (matches tm_wday)
  int64_t first_epoch, last_epoch;

  // Constructor
  TimeStruct(std::time_t unix_time, XStore_Proto::TimeUnit time_resolution);

  // Returns last day of a given month provided the year given in unixTime
  unsigned short int GetLastDayOfMonth(unsigned short int i_month) const;

  private:
    void ExtractTimeComponents();
    bool IsLeapYear() const;
};

#endif //XSTORE_TIMESTRUCTS_H
