local basexx = require('klib.encoder.basexx')
local hmac = ngx.hmac_sha1
local time, floor, char, sub = ngx.time, math.floor, string.char, string.sub
local band = require('bit').band
---@class klib.GAuth
local _M = {}

local function bytesToInt(a, b, c, d)
	return a * 0x1000000 + b * 0x10000 + c * 0x100 + d
end

---create_key create 16 base32 chars for code generating
---@param seed_str string
---@return string
function _M.create_key(seed_str)
	if #seed_str < 17 then
		seed_str = time() .. seed_str
	end
	sub(basexx.to_base32(seed_str), 2, 17)
end

function _M.get_code(key, value)
	key = basexx.from_base32(key)
	value = char(
			0, 0, 0, 0,
			band(value, 0xFF000000) / 0x1000000,
			band(value, 0xFF0000) / 0x10000,
			band(value, 0xFF00) / 0x100,
			band(value, 0xFF))
	local hash = hmac(key, value)
	local offset = band(hash:sub(-1):byte(1, 1), 0xF)
	hash = bytesToInt(hash:byte(offset + 1, offset + 4))
	hash = band(hash, 0x7FFFFFFF) % 1000000
	return ("%06d"):format(hash)
end

---get_code
---@param key string @ key
---@param refresh_interval_seconds number @ the code will refresh every 30 seconds by default, but valid for 2mins
---@return string @ 6 digit code SAME AS SHOWED in user phone device
function _M:get(key, refresh_interval_seconds)
	return _M.get_code(key, floor((time()) / (refresh_interval_seconds or 30)))
end

-- time span would be 60secs
---Check
---@param key string
---@param value string
---@param refresh_interval_seconds number @ Default 30
function _M.check(key, value, refresh_interval_seconds)
	local base = floor(time() / (refresh_interval_seconds or 30))
	if _M.get_code(key, base) == value then
		return true
	end
	if _M.get_code(key, base - 1) == value then
		return true
	end
	if _M.get_code(key, base + 1) == value then
		return true
	end
	return false
end

return _M