#include "include/DataStructs/TimeStruct.h"

// Constructor
TimeStruct::TimeStruct(std::time_t unixTime, XStore_Proto::TimeUnit timeResolution) {
    this->UnixTime = unixTime;
    this->TimeResolution = timeResolution;
    this->ExtractTimeComponents();
}

unsigned short int TimeStruct::GetLastDayOfMonth(unsigned short int iMonth) {
    return iMonth != 2 ? ((iMonth ^ (iMonth >> 3))) | 30 : IsLeapYear() ? 29 : 28;
}

bool TimeStruct::IsLeapYear() {
    return Year % 4 == 0 && (Year % 100 != 0 || Year % 400 == 0);
}

void TimeStruct::ExtractTimeComponents() {
    tm TimePtr;
    std::time_t ChoppedTime;

    if (this->TimeResolution == XStore_Proto::SECOND) {
        localtime_r(&this->UnixTime, &TimePtr);
    }
    else if (this->TimeResolution == XStore_Proto::MILLISECOND) {
        ChoppedTime = this->UnixTime / 1'000;
        localtime_r(&ChoppedTime, &TimePtr);
    }
    else if (this->TimeResolution == XStore_Proto::MICROSECOND) {
        ChoppedTime = this->UnixTime / 1'000'000;
        localtime_r(&ChoppedTime, &TimePtr);
    }
    else if (this->TimeResolution == XStore_Proto::NANOSECOND) {
        ChoppedTime = this->UnixTime / 1'000'000'000;
        localtime_r(&ChoppedTime, &TimePtr);
    }

    // Set time components
    this->Year = TimePtr.tm_year + 1900;
    this->Month = TimePtr.tm_mon + 1;
    this->Day = TimePtr.tm_mday;
    this->Hour = TimePtr.tm_hour;
    this->Minute = TimePtr.tm_min;
    this->Second = TimePtr.tm_sec;

    // Set FIRST epoch
    FirstEpochTimePtr.tm_year = this->Year - 1900;
    FirstEpochTimePtr.tm_mon = 1-1;
    FirstEpochTimePtr.tm_mday = 1;
    FirstEpochTimePtr.tm_hour = 0;
    FirstEpochTimePtr.tm_min = 0;
    FirstEpochTimePtr.tm_sec = 0;

    // Set LAST epoch
    LastEpochTimePtr.tm_year = this->Year - 1900;
    LastEpochTimePtr.tm_mon = 12-1;
    LastEpochTimePtr.tm_mday = this->GetLastDayOfMonth(12);
    LastEpochTimePtr.tm_hour = 23;
    LastEpochTimePtr.tm_min = 59;
    LastEpochTimePtr.tm_sec = 59;

    if (this->TimeResolution == XStore_Proto::SECOND) {
        FirstEpoch = mktime(&FirstEpochTimePtr);
        LastEpoch = mktime(&LastEpochTimePtr);
    }
    else if (this->TimeResolution == XStore_Proto::MILLISECOND) {
        FirstEpoch = mktime(&FirstEpochTimePtr) * 1'000;
        LastEpoch = mktime(&LastEpochTimePtr) * 1'000 + 999;
    }
    else if (this->TimeResolution == XStore_Proto::MICROSECOND) {
        FirstEpoch = mktime(&FirstEpochTimePtr) * 1'000'000;
        LastEpoch = mktime(&LastEpochTimePtr) * 1'000'000 + 999999;
    }
    else if (this->TimeResolution == XStore_Proto::NANOSECOND) {
        FirstEpoch = mktime(&FirstEpochTimePtr) * 1'000'000'000;
        LastEpoch = mktime(&LastEpochTimePtr) * 1'000'000'000 + 999999999;
    }
}