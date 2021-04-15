local sub, char, byte, find, ntime, ceil, crc32 = string.sub, string.char, string.byte, string.find, ngx.time, math.ceil, ngx.crc32_long
local utils = require('klib.utils')
local get_env, get_phase = utils.get_env, ngx.get_phase

local bec = require('klib.base.encoding')
local xxhash64_b64 = bec.xxhash64_b64
local ngx_header, ngx_ctx = ngx.header, ngx.ctx
local base = require('klib.auth.base')
local cache = require('klib.cache_manager').get('klib_auth_mfa', 1)
local ck = require "resty.cookie"
local nmatch = ngx.re.match
local time, floor = ngx.time, math.floor

---@class klib.auth.cookie:klib.auth.base
local _M = {
	cookie_name = 'g_auth',
	cookie_keep_secs = 3600 * 24,
	header_token_name = 'x-g_auth'
}

setmetatable(_M, { __index = base }) -- inherit from mfa

---refresh_cookie
---@param name string @username
---@param role number @user role in system
---@param id number @user id in system
---@param valid_secs number @keep cookie
---@return string, string @plain text and entrypted text
function _M:refresh_cookie(token, valid_secs)
	--set new token to header
	ngx_header[self.header_token] = token
	if ngx_ctx.ban_user_cookie then
		return
	end
	local cookie, err = ck:new()
	if err then
		return nil, err
	end
	local cobj = {
		key = self.cookie_name,
		value = token,
		path = "/",
		domain = utils.get_root_domain(),
		httponly = true,
		--secure = true,
		secure = false,
		--expires = ngx.cookie_time(self.cookie_keep_secs + time()),
		max_age = valid_secs or self.cookie_keep_secs
	}
	cookie:set(cobj)
	return true
end

---clear_cookie
---@param is_keep_cookie boolean @
function _M:clear_cookie(is_keep_cookie)
	ngx_ctx.ban_user_cookie = true  -- prevent cookie from regererating in different request phase
	if is_keep_cookie then
		return -- keep expired cookie
	end
	local domain = utils.get_root_domain()
	ngx_header['Set-Cookie'] = self.cookie_name .. '=;Path=/;domain=' .. domain .. ';Expires=Thu, 01-Jan-1970 00:00:00 GMT'
end

---get_current_req_token
---@param env system.env
---@return string, boolean @ token, is_header_token
function _M:get_current_req_token(env)
	local phase
	if not env then
		phase = get_phase()
		if phase == 'timer' then
			return self.default_timer_token, true
		end
		env = get_env()
	end
	local str, err = env.header[self.header_token_name]
	if not str then
		local mc = nmatch(env.header['cookie'], self.cookie_name .. [[=([^;]+)]])
		if mc then
			str = mc[1]
		end
	else
		return str, true
	end
	return str, false
end

return _M