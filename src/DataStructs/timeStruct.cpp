#include "include/DataStructs/timeStruct.h"

// Constructor
timeStruct::timeStruct(std::time_t unixTime, XStore_Proto::TimeUnit timeResolution) {
    this->unixTime = unixTime;
    this->timeResolution = timeResolution;
    this->extractTimeComponents();
}

unsigned short int timeStruct::getLastDayOfMonth(unsigned short int iMonth) {
    return iMonth != 2 ? ((iMonth ^ (iMonth >> 3))) | 30 : isLeapYear() ? 29 : 28;
}

bool timeStruct::isLeapYear() {
    return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

void timeStruct::extractTimeComponents() {
    tm timePtr;
    std::time_t choppedTime;

    if (this->timeResolution == XStore_Proto::SECOND) {
        localtime_r(&this->unixTime, &timePtr);
    }
    else if (this->timeResolution == XStore_Proto::MILLISECOND) {
        choppedTime = this->unixTime / 1'000;
        localtime_r(&choppedTime, &timePtr);
    }
    else if (this->timeResolution == XStore_Proto::MICROSECOND) {
        choppedTime = this->unixTime / 1'000'000;
        localtime_r(&choppedTime, &timePtr);
    }
    else if (this->timeResolution == XStore_Proto::NANOSECOND) {
        choppedTime = this->unixTime / 1'000'000'000;
        localtime_r(&choppedTime, &timePtr);
    }

    // Set time components
    this->year = timePtr.tm_year + 1900;
    this->month = timePtr.tm_mon + 1;
    this->day = timePtr.tm_mday;
    this->hour = timePtr.tm_hour;
    this->minute = timePtr.tm_min;
    this->second = timePtr.tm_sec;

    // Set FIRST epoch
    firstEpochTimePtr.tm_year = this->year - 1900;
    firstEpochTimePtr.tm_mon = 1-1;
    firstEpochTimePtr.tm_mday = 1;
    firstEpochTimePtr.tm_hour = 0;
    firstEpochTimePtr.tm_min = 0;
    firstEpochTimePtr.tm_sec = 0;

    // Set LAST epoch
    lastEpochTimePtr.tm_year = this->year - 1900;
    lastEpochTimePtr.tm_mon = 12-1;
    lastEpochTimePtr.tm_mday = this->getLastDayOfMonth(12);
    lastEpochTimePtr.tm_hour = 23;
    lastEpochTimePtr.tm_min = 59;
    lastEpochTimePtr.tm_sec = 59;

    if (this->timeResolution == XStore_Proto::SECOND) {
        firstEpoch = mktime(&firstEpochTimePtr);
        lastEpoch = mktime(&lastEpochTimePtr);
    }
    else if (this->timeResolution == XStore_Proto::MILLISECOND) {
        firstEpoch = mktime(&firstEpochTimePtr) * 1'000;
        lastEpoch = mktime(&lastEpochTimePtr) * 1'000 + 999;
    }
    else if (this->timeResolution == XStore_Proto::MICROSECOND) {
        firstEpoch = mktime(&firstEpochTimePtr) * 1'000'000;
        lastEpoch = mktime(&lastEpochTimePtr) * 1'000'000 + 999999;
    }
    else if (this->timeResolution == XStore_Proto::NANOSECOND) {
        firstEpoch = mktime(&firstEpochTimePtr) * 1'000'000'000;
        lastEpoch = mktime(&lastEpochTimePtr) * 1'000'000'000 + 999999999;
    }
}