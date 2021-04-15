--[[

--]]
local sub, char, byte, find, ntime, ceil, crc32, is_empty = string.sub, string.char, string.byte, string.find, ngx.time, math.ceil, ngx.crc32_long, string.is_empty
local utils = require('klib.utils')
local bec = require('klib.base.encoding')
local xxhash64_b64 = bec.xxhash64_b64
local ckbase = require('klib.auth.cookie')
local region = require('klib.biz.region')
local get_env = utils.get_env
local event_bus = require('klib.biz.event_bus')
local array, hash = table.array, table.hash
local checker = require('klib.auth.checker')
local tag = '[AUTH]'

local policy = checker.CHECK_POLICY
local result = checker.CHECK_RESULT
local default_policy = policy.ROUGH_IP

---@class klib.auth.init:klib.auth.cookie
local _M = {
	aes_key = 'init_secure_key',
	cookie_name = '_g_auth',
	cookie_keep_secs = 3600 * 24,
	auth_duration = 6 * 3600,
	auth_extend_in = 4 * 3600, -- 24 hour / 64
	header_token_name = 'x-gtoken',
	mode = 1,
	track_region = true,
	enable_ase_entrypt = false,
	start_timestamp = 1559000000, -- 2019-01-21 16:00:00
	cache = ngx.shared['cache'],
	default_timer_token = '',
	checker = checker,
	check_policy = policy.CHECK_FINGERPRINT,
}

setmetatable(_M, { __index = ckbase })

--1548057600
--1528057600
--1516158000
--  20057600
--  16777216
--  31536000 -- 1year
---new
---@param key string @ [default: secure_key, required] key to generate token
---@param cookie_name string @ [default: _g_auth, required] cookie name to store token
---@param cookie_keep_secs number @[default: 3600 * 24, required] cookie kept seconds, may invalid to auth, but for tracking
---@param auth_duration number @ [default: 60 * 21, required] force token to refresh every `xxx second`
---@param auth_extend_in number @ [default: 60 * 21, required] cookie kept seconds
---@param header_token_name string @ [default: x-gtoken, required] header-key to transfer token by header
---@param start_timestamp number @ [default: 1548057600, required] default timestamp to calculate token
---@param check_policy number @ [default: 1, required] 1: strict, 2: loosen, 3:medium 1: strict, 2: loosen, 3:medium
function _M.new(key, cookie_name, cookie_keep_secs, auth_duration, auth_extend_in, header_token_name, start_timestamp, check_policy)
	local inst = ckbase.new(key, start_timestamp or _M.start_timestamp)
	inst.cookie_name = cookie_name or _M.cookie_name
	inst.cookie_keep_secs = cookie_keep_secs or _M.cookie_keep_secs
	inst.auth_duration = auth_duration or _M.auth_duration
	inst.auth_extend_in = auth_extend_in or _M.auth_extend_span
	inst.header_token_name = header_token_name or _M.header_token_name
	inst.check_policy = check_policy or _M.check_policy
	setmetatable(inst, { __index = _M })
	inst.default_timer_token = inst:create_token(0, 'timer', 0, 0, 3600 * 24 * 1000)
	return inst
end

function _M.refresh_token(encrypted_token)

end

---base_login Least password or mfa authenticate method required
---@param salt string @ [Nullable] for hashing password with user's salt
---@param password string @ [Nullable] user inputted plain password
---@param hashed_password string @ [Nullable] hashed user password stored in server database
---@param mfa_key string @ [Nullable] user mfa key for MFA auth
---@param mfa_code string @ [Nullable] 6 digit code string for MFA auth
---@param extra_validation string @ [Nullable] extra string for MFA lock, usually by using user-agent and IP
---@return boolean, string @ success, error message
function _M:base_login(salt, password, hashed_password, mfa_key, mfa_code, extra_validation)
	local logged = false
	if salt and password and hashed_password then
		if self:hash_password(password, salt) ~= hashed_password then
			return false, 'bad password'
		end
		logged = true
	end
	if mfa_key and mfa_code then
		if not self:check_mfa_code(mfa_key, mfa_code, extra_validation) then
			return false, 'bad auth code'
		end
		logged = true
	end
	if not logged then
		return false, 'mfa or password authentication required'
	end
	--self:refresh_cookie(self:create_token())
	return true
end

-----update_token
-----@param env system.env
--function _M:update_token(env)
--	env = env or get_env()
--	local region_bin = region.get_region_data(env.ip)
--	local token = self:create_token()
--end

---check
---@param env system.env
---@param policy_num number
---@param token string
---@return boolean, number, number, string, klib.auth.region_info, number @ success, ttl, id, name, region_bytes, hashed_finger_print
function _M:check(env, policy_num, token)
	env = env or get_env()
	local req_ip = env.ip
	token = token or self:get_current_req_token(env)
	if not token then
		return false, result.EMPTY_TOKEN
	end
	local hash_code
	if policy.contain(policy_num, policy.CHECK_FINGERPRINT) then
		local ua = env.header['user-agent']
		hash_code = bec.uint_byte(self:hash(ua))
	end
	return self.checker.check(self, policy_num or default_policy, token, req_ip, hash_code)
end

---get_user from current request
---@param env system.env
---@param policy_num number
function _M:get_user(env, policy_num)
	local usr = ngx.ctx.__user
	if usr then
		if policy.any(policy_num, policy.CHECK_LEVEL, policy.CHECK_ROLE) and usr.role then
			return usr
		end
	end

	local ok, ttl, id, name, region_info, finger_print_hash, role, level = self:check(env, policy_num)
	if not ok then
		return nil, ttl -- ttl is the check result
	end
	return {
		id = id,
		name = name,
		region_info = region_info,
		role = role,
		ttl = ttl,
		level = level
	}
end

---extend_get_user
---@param env system.env
---@param policy_num number
---@param force_refresh boolean
---@param callback fun(callback_arg:table):klib.auth.user
---@param callback_arg table
---@return klib.auth.user
function _M:extend_get_user(env, policy_num, force_refresh, callback, callback_arg)
	local usr, err = self:get_user(env, policy_num)
	if err and err ~= result.EMPTY_TOKEN then
		return nil, err
	end

	local need_extend
	if not usr or usr.ttl < self.auth_extend_in then
		need_extend = true
	end
	if need_extend or force_refresh then
		if callback then
			usr = callback(callback_arg)
		elseif not usr.role then
			usr, err = self:get_user(env, policy.append(policy_num, policy.CHECK_ROLE))
			if err then
				return nil, err
			end
		end
		local token = self:create_token(usr.id, usr.name, usr.role, usr.level, self.auth_extend_span)
		self:refresh_cookie(token, self.cookie_keep_secs)
	end
	return usr
end

---logout
function _M:logout()
	self:clear_cookie(true)
end

return _M

---@class klib.auth.user
---@field id number
---@field name number
---@field role number
---@field level number
---@field ttl number
---@field region_info klib.auth.region_info