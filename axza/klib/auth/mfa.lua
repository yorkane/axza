local sub, char, byte, find, ntime, ceil, crc32 = string.sub, string.char, string.byte, string.find, ngx.time, math.ceil, ngx.crc32_long
local utils = require('klib.utils')
local bec = require('klib.base.encoding')
local xxhash64_b64 = bec.xxhash64_b64
local mfa = require("klib.auth.mfa_code")
local basexx = require('klib.encoder.basexx')
local cache = require('klib.cache_manager').get('klib_auth_mfa', 1)
local time, floor = ngx.time, math.floor

---@class klib.auth.mfa
local _M = {
	mfa_refresh_seconds = 30
}

---check_code prevent using authcode more than once
---@param mfa_key string @ 16 base32 chars which generated mfa_code
---@param mfa_code string @ 6 digit code
---@param extra_validation string @ for preventing different client to request same mfa code validation for 60 seconds
---@return boolean
function _M:check_mfa_code(mfa_key, mfa_code, extra_validation)
	local key
	if extra_validation then
		key = mfa_key .. mfa_code
		local v = cache:get(key)
		if v and v ~= extra_validation then
			return false
		end
		cache:set(key, extra_validation, 60)
	end
	return mfa.check(mfa_key, mfa_code, self.mfa_refresh_seconds)
end

---generate_key
---@param username string @unique user name
---@param mail string @unique user email
---@return string @ genertated key with timestamp, username, email factors
function _M:create_mfa_key(username, mail)
	local digest = xxhash64_b64(username .. time()) .. mail
	digest = sub(basexx.to_base32(digest), 2, 17)
	return digest
end

---get_code
---@param mfa_key string @ user secure key
---@return string @ 6 digit code SAME AS SHOWED in user phone device
function _M:get_mfa_code(mfa_key)
	return mfa.get_code(mfa_key, floor((time()) / self.mfa_refresh_seconds))
end

return _M