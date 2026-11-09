#pragma once
#include "atlas/service.hpp"
#include <span>

namespace atlas::wire {
constexpr std::size_t header_size = 10;
uint32_t length(std::span<const uint8_t> data);
Request decode(std::vector<uint8_t> body);
std::vector<uint8_t> encode(uint8_t type, uint64_t id, const Response &response);
Response error(uint16_t status, const std::string &text);
} // namespace atlas::wire
