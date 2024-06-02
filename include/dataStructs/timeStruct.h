#ifndef XSTORE_TIMESTRUCTS_H
#define XSTORE_TIMESTRUCTS_H

#include <iostream>
#include <ctime>
#include <ctime>

// Data structure to hold time-value together
struct timeStruct {
  std::time_t unixTime;
  std::string timeResolution;
  unsigned short int year, month, day, hour, minute, second;
  long firstEpoch, lastEpoch;

  // Constructor
  timeStruct(std::time_t unixTime);

  // Returns last day of a given month provided the year given in unixTime
  unsigned short int getLastDayOfMonth(unsigned short int iMonth);

  private:
//    unsigned short int firstYear, firstMonth, firstDay, firstHour, firstMinute, firstSecond;
    tm firstEpochTimePtr{};
    tm lastEpochTimePtr{};
    void getTimeResolution();
    void extractTimeComponents();
    bool isLeapYear();
};

#endif //XSTORE_TIMESTRUCTS_H
