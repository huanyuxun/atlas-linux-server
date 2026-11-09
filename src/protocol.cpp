#include "atlas/protocol.hpp"
#include <stdexcept>

namespace atlas::wire {
uint32_t length(std::span<const uint8_t> data) {
    if (data.size() != 4)
        throw std::invalid_argument("length header must be 4 bytes");
    uint32_t n = 0;
    for (auto b : data)
        n = (n << 8) | b;
    return n;
}
Request decode(std::vector<uint8_t> body) {
    if (body.size() < header_size || body[0] != 1)
        throw std::invalid_argument("invalid protocol version/header");
    Request r{};
    r.type = body[1];
    r.id = 0;
    for (std::size_t i = 2; i < 10; ++i)
        r.id = (r.id << 8) | body[i];
    body.erase(body.begin(), body.begin() + 10);
    r.payload = std::move(body);
    return r;
}
std::vector<uint8_t> encode(uint8_t type, uint64_t id, const Response &r) {
    auto n = static_cast<uint32_t>(header_size + 2 + r.payload.size());
    std::vector<uint8_t> out;
    out.reserve(n + 4);
    for (int s = 24; s >= 0; s -= 8)
        out.push_back(static_cast<uint8_t>(n >> s));
    out.push_back(1);
    out.push_back(type | 0x80);
    for (int s = 56; s >= 0; s -= 8)
        out.push_back(static_cast<uint8_t>(id >> s));
    out.push_back(static_cast<uint8_t>(r.status >> 8));
    out.push_back(static_cast<uint8_t>(r.status));
    out.insert(out.end(), r.payload.begin(), r.payload.end());
    return out;
}
Response error(uint16_t status, const std::string &text) {
    return {status, {text.begin(), text.end()}};
}
} // namespace atlas::wire
