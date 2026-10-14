#pragma once
// The only way to include p-ranav/indicators: v2.3's termcolor.hpp uses uint8_t without including <cstdint>, which
// newer GCC no longer provides transitively.
#include <cstdint>
#include <indicators/progress_bar.hpp>
