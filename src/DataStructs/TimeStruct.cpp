#include "include/DataStructs/TimeStruct.h"
#include <stdexcept>

namespace {

// Converts a value already expressed in `resolution` units (i.e. UnixTime)
// down to whole seconds since epoch. Sub-second resolutions divide,
// super-second resolutions multiply.
constexpr int64_t ToSeconds(int64_t value, XStore_Proto::TimeUnit resolution) {
    switch (resolution) {
        case XStore_Proto::SECOND:      return value;
        case XStore_Proto::MILLISECOND: return value / 1'000;
        case XStore_Proto::MICROSECOND: return value / 1'000'000;
        case XStore_Proto::NANOSECOND:  return value / 1'000'000'000;
        case XStore_Proto::MINUTE:      return value * 60;
        case XStore_Proto::HOUR:        return value * 3'600;
        case XStore_Proto::DAY:         return value * 86'400;
        default:
            throw std::invalid_argument("TimeStruct: unrecognized TimeResolution");
    }
}

// Inverse of ToSeconds. When atUnitEnd is set, lands on the unit that
// contains that second's final instant instead of its first -- used for
// LastEpoch, since a whole millisecond/minute/etc. spans more than one
// underlying second-boundary.
constexpr int64_t FromSeconds(int64_t seconds, XStore_Proto::TimeUnit resolution, bool atUnitEnd) {
    switch (resolution) {
        case XStore_Proto::SECOND:      return seconds;
        case XStore_Proto::MILLISECOND: return seconds * 1'000 + (atUnitEnd ? 999 : 0);
        case XStore_Proto::MICROSECOND: return seconds * 1'000'000 + (atUnitEnd ? 999'999 : 0);
        case XStore_Proto::NANOSECOND:  return seconds * 1'000'000'000 + (atUnitEnd ? 999'999'999 : 0);
        case XStore_Proto::MINUTE:      return seconds / 60;
        case XStore_Proto::HOUR:        return seconds / 3'600;
        case XStore_Proto::DAY:         return seconds / 86'400;
        default:
            throw std::invalid_argument("TimeStruct: unrecognized TimeResolution");
    }
}

// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian civil
// date. Howard Hinnant's days_from_civil -- pure integer arithmetic, no
// tzdata/libc calendar calls. This is what lets FirstEpoch/LastEpoch be
// computed without mktime()/timegm().
constexpr int64_t DaysFromCivil(int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int64_t>(doe) - 719468;
}

constexpr unsigned short kDaysInMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

// Floor division for a positive divisor. Plain `/` truncates toward zero,
// which for negative `a` (pre-1970 seconds) rounds the day index *up*
// instead of down and desyncs it from the actual calendar day.
constexpr int64_t FloorDiv(int64_t a, int64_t b) {
    const int64_t q = a / b;
    const int64_t r = a % b;
    return r < 0 ? q - 1 : q;
}

// Inverse of DaysFromCivil -- proleptic-Gregorian civil date for a given
// day count since the Unix epoch. Same Hinnant algorithm pair, so it stays
// exactly consistent with DaysFromCivil's leap-year handling.
constexpr int64_t CivilFromDays(int64_t z, unsigned &outMonth, unsigned &outDay) {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const int64_t y = static_cast<int64_t>(yoe) + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    outMonth = mp + (mp < 10 ? 3 : static_cast<unsigned>(-9));
    outDay = doy - (153 * mp + 2) / 5 + 1;
    return y + (outMonth <= 2);
}

// 1970-01-01 (day 0) was a Thursday (tm_wday 4); this recovers the same
// 0=Sunday..6=Saturday numbering directly from the day count.
constexpr unsigned WeekdayFromDays(int64_t z) {
    return static_cast<unsigned>(z >= -4 ? (z + 4) % 7 : (z + 5) % 7 + 6);
}

} // namespace

// Constructor
TimeStruct::TimeStruct(std::time_t unixTime, XStore_Proto::TimeUnit timeResolution) {
    this->UnixTime = unixTime;
    this->TimeResolution = timeResolution;
    this->ExtractTimeComponents();
}

unsigned short int TimeStruct::GetLastDayOfMonth(unsigned short int iMonth) const {
    return iMonth != 2 ? kDaysInMonth[iMonth - 1] : (IsLeapYear() ? 29 : 28);
}

bool TimeStruct::IsLeapYear() const {
    return Year % 4 == 0 && (Year % 100 != 0 || Year % 400 == 0);
}

void TimeStruct::ExtractTimeComponents() {
    const int64_t SecondsSinceEpoch = ToSeconds(this->UnixTime, this->TimeResolution);

    // Closed-form UTC calendar extraction -- no gmtime_r()/libc calendar
    // call. Deterministic regardless of host TZ/DST, since FirstEpoch/
    // LastEpoch drive on-disk row addressing.
    const int64_t DaysSinceEpoch = FloorDiv(SecondsSinceEpoch, 86'400);
    const int64_t SecondsOfDay = SecondsSinceEpoch - DaysSinceEpoch * 86'400;

    unsigned Month, Day;
    this->Year = static_cast<unsigned short>(CivilFromDays(DaysSinceEpoch, Month, Day));
    this->Month = static_cast<unsigned short>(Month);
    this->Day = static_cast<unsigned short>(Day);
    this->Hour = static_cast<unsigned short>(SecondsOfDay / 3600);
    this->Minute = static_cast<unsigned short>((SecondsOfDay % 3600) / 60);
    this->Second = static_cast<unsigned short>(SecondsOfDay % 60);
    this->Weekday = static_cast<unsigned short>(WeekdayFromDays(DaysSinceEpoch));

    // Year's [start, end] bound in seconds, computed directly from the civil
    // calendar -- no mktime()/timegm() call needed. LastEpochSeconds is
    // derived from FirstEpochSeconds + year length rather than a second
    // DaysFromCivil call, since Year (and IsLeapYear()) is already known.
    const int64_t FirstEpochSeconds = DaysFromCivil(this->Year, 1, 1) * 86'400;
    const int64_t YearLengthDays = IsLeapYear() ? 366 : 365;
    const int64_t LastEpochSeconds = FirstEpochSeconds + YearLengthDays * 86'400 - 1;

    this->FirstEpoch = FromSeconds(FirstEpochSeconds, this->TimeResolution, false);
    this->LastEpoch = FromSeconds(LastEpochSeconds, this->TimeResolution, true);
}
