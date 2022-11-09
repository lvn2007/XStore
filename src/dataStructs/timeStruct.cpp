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

    if (count >= 10 && count < 13) {
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
    tm* timePtr;
    if (this->timeResolution == "SECOND") {
        timePtr = localtime(&this->unixTime);
    }
    else if (this->timeResolution == "MILLISECOND") {
        std::time_t choppedTime = this->unixTime / 1000;
        timePtr = localtime(&choppedTime);
    }
    else if (this->timeResolution == "MICROSECOND") {
        std::time_t choppedTime = this->unixTime / 1000000;
        timePtr = localtime(&choppedTime);
    }

    // Set time components
    this->year = timePtr->tm_year + 1900;
    this->month = timePtr->tm_mon + 1;
    this->day = timePtr->tm_mday;
    this->hour = timePtr->tm_hour;
    this->minute = timePtr->tm_min;
    this->second = timePtr->tm_sec;
}