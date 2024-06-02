#include "include/dataStructs/timeStruct.h"

// Constructor
timeStruct::timeStruct(std::time_t unixTime) {
    this->unixTime = unixTime;
    this->getTimeResolution();
    this->extractTimeComponents();
}

unsigned short int timeStruct::getLastDayOfMonth(unsigned short int iMonth) {
    return iMonth != 2 ? ((iMonth ^ (iMonth >> 3))) | 30 : isLeapYear() ? 29 : 28;
}

bool timeStruct::isLeapYear() {
    return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

void timeStruct::getTimeResolution() {
    int count = 0;
    std::time_t localUnixTime = this->unixTime;

    while (localUnixTime != 0) {
        localUnixTime /= 10;
        ++count;
    }

    if (count < 13) {
        this->timeResolution = "SECOND";
        return;
    }
    else if (count >= 13 && count < 16) {
        this->timeResolution = "MILLISECOND";
        return;
    }
    else if (count >= 16) {
        this->timeResolution = "MICROSECOND";
        return;
    }
    this->timeResolution = "NULL";
}

void timeStruct::extractTimeComponents() {
    tm timePtr;
    std::time_t choppedTime;

    if (this->timeResolution == "SECOND") {
        localtime_r(&this->unixTime, &timePtr);
    }
    else if (this->timeResolution == "MILLISECOND") {
        choppedTime = this->unixTime / 1000;
        localtime_r(&choppedTime, &timePtr);
    }
    else if (this->timeResolution == "MICROSECOND") {
        choppedTime = this->unixTime / 1000000;
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

    if (this->timeResolution == "SECOND") {
        firstEpoch = mktime(&firstEpochTimePtr);
        lastEpoch = mktime(&lastEpochTimePtr);
    }
    else if (this->timeResolution == "MILLISECOND") {
        firstEpoch = mktime(&firstEpochTimePtr) * 1000;
        lastEpoch = mktime(&lastEpochTimePtr) * 1000 + 999;
    }
    else if (this->timeResolution == "MICROSECOND") {
        firstEpoch = mktime(&firstEpochTimePtr) * 1000000;
        lastEpoch = mktime(&lastEpochTimePtr) * 1000000 + 999999;
    }
}