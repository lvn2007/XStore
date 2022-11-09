#ifndef XSTORE_TIMESTRUCTS_H
#define XSTORE_TIMESTRUCTS_H

#include <iostream>
#include <ctime>
#include <time.h>

// Data structure to hold time-value together
struct timeStruct {
  std::time_t unixTime;
  std::string timeResolution;
  unsigned short int year, month, day, hour, minute, second;

  // Constructor
  timeStruct(std::time_t unixTime);

  // Returns last day of a given month provided the year given in unixTime
  unsigned short int getLastDayOfMonth(unsigned short int iMonth);

  private:
    void getTimeResolution();
    void extractTimeComponents();
    bool isLeapYear();
};

#endif //XSTORE_TIMESTRUCTS_H
