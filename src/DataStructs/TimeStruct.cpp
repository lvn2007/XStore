#include "include/DataStructs/TimeStruct.h"
#include "include/Logger/Logger.h"
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
            // Never legitimately reachable -- CreateDB already validates
            // resolution against PermittedTimeResolution before a CfgStruct
            // is ever written, so this only fires on a corrupted/hand-edited
            // .CFG file. Logged (not just thrown) since the exception may
            // propagate past a caller that doesn't log its .what().
            spdlog::critical("[TIME STRUCT] - Unrecognized TimeResolution value: {}", static_cast<int>(resolution));
            throw std::invalid_argument("TimeStruct: unrecognized TimeResolution");
    }
}

// Inverse of ToSeconds. When atUnitEnd is set, lands on the unit that
// contains that second's final instant instead of its first -- used for
// LastEpoch, since a whole millisecond/minute/etc. spans more than one
// underlying second-boundary.
constexpr int64_t FromSeconds(int64_t seconds, XStore_Proto::TimeUnit resolution, bool at_unit_end) {
    switch (resolution) {
        case XStore_Proto::SECOND:      return seconds;
        case XStore_Proto::MILLISECOND: return seconds * 1'000 + (at_unit_end ? 999 : 0);
        case XStore_Proto::MICROSECOND: return seconds * 1'000'000 + (at_unit_end ? 999'999 : 0);
        case XStore_Proto::NANOSECOND:  return seconds * 1'000'000'000 + (at_unit_end ? 999'999'999 : 0);
        case XStore_Proto::MINUTE:      return seconds / 60;
        case XStore_Proto::HOUR:        return seconds / 3'600;
        case XStore_Proto::DAY:         return seconds / 86'400;
        default:
            // Never legitimately reachable -- CreateDB already validates
            // resolution against PermittedTimeResolution before a CfgStruct
            // is ever written, so this only fires on a corrupted/hand-edited
            // .CFG file. Logged (not just thrown) since the exception may
            // propagate past a caller that doesn't log its .what().
            spdlog::critical("[TIME STRUCT] - Unrecognized TimeResolution value: {}", static_cast<int>(resolution));
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

constexpr unsigned short K_DAYS_IN_MONTH[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

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
constexpr int64_t CivilFromDays(int64_t z, unsigned &out_month, unsigned &out_day) {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const int64_t y = static_cast<int64_t>(yoe) + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    out_month = mp + (mp < 10 ? 3 : static_cast<unsigned>(-9));
    out_day = doy - (153 * mp + 2) / 5 + 1;
    return y + (out_month <= 2);
}

// 1970-01-01 (day 0) was a Thursday (tm_wday 4); this recovers the same
// 0=Sunday..6=Saturday numbering directly from the day count.
constexpr unsigned WeekdayFromDays(int64_t z) {
    return static_cast<unsigned>(z >= -4 ? (z + 4) % 7 : (z + 5) % 7 + 6);
}

} // namespace

// Constructor
TimeStruct::TimeStruct(std::time_t unix_time, XStore_Proto::TimeUnit time_resolution) {
    this->unix_time = unix_time;
    this->time_resolution = time_resolution;
    this->ExtractTimeComponents();
}

unsigned short int TimeStruct::GetLastDayOfMonth(unsigned short int i_month) const {
    return i_month != 2 ? K_DAYS_IN_MONTH[i_month - 1] : (IsLeapYear() ? 29 : 28);
}

bool TimeStruct::IsLeapYear() const {
    return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

void TimeStruct::ExtractTimeComponents() {
    const int64_t seconds_since_epoch = ToSeconds(this->unix_time, this->time_resolution);

    // Closed-form UTC calendar extraction -- no gmtime_r()/libc calendar
    // call. Deterministic regardless of host TZ/DST, since FirstEpoch/
    // LastEpoch drive on-disk row addressing.
    const int64_t days_since_epoch = FloorDiv(seconds_since_epoch, 86'400);
    const int64_t seconds_of_day = seconds_since_epoch - days_since_epoch * 86'400;

    unsigned month, day;
    this->year = static_cast<unsigned short>(CivilFromDays(days_since_epoch, month, day));
    this->month = static_cast<unsigned short>(month);
    this->day = static_cast<unsigned short>(day);
    this->hour = static_cast<unsigned short>(seconds_of_day / 3600);
    this->minute = static_cast<unsigned short>((seconds_of_day % 3600) / 60);
    this->second = static_cast<unsigned short>(seconds_of_day % 60);
    this->weekday = static_cast<unsigned short>(WeekdayFromDays(days_since_epoch));

    // Year's [start, end] bound in seconds, computed directly from the civil
    // calendar -- no mktime()/timegm() call needed. LastEpochSeconds is
    // derived from FirstEpochSeconds + year length rather than a second
    // DaysFromCivil call, since Year (and IsLeapYear()) is already known.
    const int64_t first_epoch_seconds = DaysFromCivil(this->year, 1, 1) * 86'400;
    const int64_t year_length_days = IsLeapYear() ? 366 : 365;
    const int64_t last_epoch_seconds = first_epoch_seconds + year_length_days * 86'400 - 1;

    this->first_epoch = FromSeconds(first_epoch_seconds, this->time_resolution, false);
    this->last_epoch = FromSeconds(last_epoch_seconds, this->time_resolution, true);
}
