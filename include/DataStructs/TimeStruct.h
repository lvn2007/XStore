#ifndef XSTORE_TIMESTRUCTS_H
#define XSTORE_TIMESTRUCTS_H

#include <iostream>
#include <ctime>
#include <ctime>
#include "XStore.pb.h"

// Data structure to hold time-value together
struct TimeStruct {
  std::time_t UnixTime;
  XStore_Proto::TimeUnit TimeResolution;
  unsigned short int Year, Month, Day, Hour, Minute, Second;
  long FirstEpoch, LastEpoch;

  // Constructor
  TimeStruct(std::time_t unixTime, XStore_Proto::TimeUnit timeResolution);

  // Returns last day of a given month provided the year given in unixTime
  unsigned short int GetLastDayOfMonth(unsigned short int iMonth);

  private:
    tm FirstEpochTimePtr{};
    tm LastEpochTimePtr{};
    void ExtractTimeComponents();
    bool IsLeapYear();
};

#endif //XSTORE_TIMESTRUCTS_H
