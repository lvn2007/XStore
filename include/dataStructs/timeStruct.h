#ifndef XSTORE_TIMESTRUCTS_H
#define XSTORE_TIMESTRUCTS_H

#include <iostream>
#include <ctime>
#include <ctime>
#include "XStore.pb.h"

// Data structure to hold time-value together
struct timeStruct {
  std::time_t unixTime;
  XStore_Proto::timeUnit timeResolution;
  unsigned short int year, month, day, hour, minute, second;
  long firstEpoch, lastEpoch;

  // Constructor
  timeStruct(std::time_t unixTime, XStore_Proto::timeUnit timeResolution);

  // Returns last day of a given month provided the year given in unixTime
  unsigned short int getLastDayOfMonth(unsigned short int iMonth);

  private:
    tm firstEpochTimePtr{};
    tm lastEpochTimePtr{};
    void extractTimeComponents();
    bool isLeapYear();
};

#endif //XSTORE_TIMESTRUCTS_H
