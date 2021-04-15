local sub, char, byte, find, ntime, ceil, crc32 = string.sub, string.char, string.byte, string.find, ngx.time, math.ceil, ngx.crc32_long
local utils = require('klib.utils')
local em = require('klib.base.enum')
local region = require('klib.biz.region')
local ip2region = require('klib.biz.ip2region')
local bec = require('klib.base.encoding')
local xxhash32, ip_byte = bec.xxhash32, ip2region.ip_byte
local contain, any, get_region_data, decompress_region = em.contain, em.any, region.get_region_data, region.decompress_region
local time, floor = ngx.time, math.floor
local hash, array, int_byte = table.hash, table.array

---@class klib.auth.policy:klib.base.enum
---@field SAME_IP number @ match whole ip
---@field ROUGH_IP number @ match first 3 digits, ignore last digit
---@field CHECK_STATE number @ same region
---@field CHECK_NATION number @ same nation
---@field CHECK_CITY number @ same city
---@field CHECK_DISTRICT number @ same DISTRICT
---@field CHECK_FINGERPRINT number @ match whole finger print hash
---@field CHECK_SERVICE_PROVIDER number @ match service provider id
local POLICY = {
	SAME_IP = 0,
	ROUGH_IP = 0,
	CHECK_ROLE = 0,
	CHECK_LEVEL = 0,
	CHECK_NATION = 0,
	CHECK_STATE = 0,
	CHECK_CITY = 0,
	CHECK_DISTRICT = 0,
	CHECK_FINGERPRINT = 0,
	CHECK_SERVICE_PROVIDER = 0,
}

em.new(POLICY)

---@class klib.auth.result:klib.base.enum
local CHECK_RESULT = {
	EMPTY_TOKEN = 0,
	BAD_TOKEN = 0,
	BAD_AUTH_CODE = 0,
	LOGIN_EXPIRED = 0,
	NEED_EXTEND = 0,
	HACKED_TOKEN = 0,
	CHANGED_SP = 0,
	CHANGED_IP = 0,
	CHANGED_ROUGH_IP = 0,
	CHANGED_NATION = 0,
	CHANGED_STATE = 0,
	CHANGED_CITY = 0,
	CHANGED_DISTRICT = 0,
	CHANGED_FINGERPRINT = 0
}

em.new(CHECK_RESULT)

---@class klib.auth.checker
local _M = {
	CHECK_POLICY = POLICY,
	CHECK_RESULT = CHECK_RESULT,
}
local mt = { __index = _M }

---check
---@param auth_inst klib.auth.init
---@param policy number
---@see klib.auth.policy
---@param encrypted_token string
---@param env system.env
---@return boolean, number, number, string, klib.auth.region_info, number, number, number @ success, ttl, id, name, region_info, hashed_finger_print, role, level
function _M.check(auth_inst, policy, encrypted_token, req_ip, finger_print_hashed)
	local ttl, id, name, region_bytes, hashed_finger_print = auth_inst:extract_plain_token(encrypted_token, true, true, true, true)
	if ttl < 1 then
		return false, CHECK_RESULT.LOGIN_EXPIRED
	end
	local ok, id2, role, level, exptime, region_info

	if any(policy, POLICY.CHECK_ROLE, POLICY.CHECK_LEVEL) then
		id2, role, level, exptime = auth_inst:decrypt_token(encrypted_token)
		if not id2 or id ~= id2 or exptime < ngx.time() then
			return false, CHECK_RESULT.HACKED_TOKEN
		end
	end

	if finger_print_hashed and contain(policy, POLICY.CHECK_FINGERPRINT) and hashed_finger_print ~= finger_print_hashed then
		return false, CHECK_RESULT.CHANGED_FINGERPRINT
	end
	if req_ip then
		req_ip = ip_byte(req_ip) -- convert any ip forms into byte
		if contain(policy, POLICY.SAME_IP) then
			local ip = sub(region_bytes, 1, 4)
			if ip ~= req_ip then
				return false, CHECK_RESULT.CHANGED_IP
			end
		elseif contain(policy, POLICY.ROUGH_IP) then
			local ip = sub(region_bytes, 1, 3)
			--dump(policy, encryted_token, req_ip, finger_print_hashed)
			if ip ~= sub(req_ip, 1, 3) then
				return false, CHECK_RESULT.CHANGED_ROUGH_IP
			end
		end
		if any(policy, POLICY.CHECK_STATE, POLICY.CHECK_CITY, POLICY.CHECK_DISTRICT, POLICY.CHECK_NATION, POLICY.CHECK_SERVICE_PROVIDER) then
			ok, region_info = _M.check_region(policy, req_ip, region_bytes)
			if not ok then
				return ok, region_info
			end
		end
	end
	return true, ttl, id, name, region_info, hashed_finger_print, role, level
end

---check_region
---@param policy number
---@param req_ip string
---@param region_bytes string
function _M.check_region(policy, req_ip, region_bytes)
	local ip1, nation_id1, state_id1, city_id1, district_id1, service_provider_id1 = get_region_data(req_ip, region.DATA_TYPE.SEPARATED_INFO)
	local ip, nation_id, state_id, city_id, district_id, sp_id = decompress_region(region_bytes)
	if em.contain(policy, POLICY.CHECK_SERVICE_PROVIDER) and service_provider_id1 ~= sp_id then
		return false, CHECK_RESULT.CHANGED_SP
	end
	if em.contain(policy, POLICY.CHECK_NATION) and nation_id ~= nation_id1 then
		return false, CHECK_RESULT.CHANGED_NATION
	end
	if em.contain(policy, POLICY.CHECK_STATE) and state_id ~= state_id1 then
		return false, CHECK_RESULT.CHANGED_STATE
	end
	if em.contain(policy, POLICY.CHECK_CITY) and city_id ~= city_id1 then
		return false, CHECK_RESULT.CHANGED_CITY
	end
	if em.contain(policy, POLICY.CHECK_DISTRICT) and district_id ~= district_id1 then
		return false, CHECK_RESULT.CHANGED_DISTRICT
	end
	return true, {
		nation_id = nation_id,
		state_id = state_id,
		city_id = city_id,
		district_id = district_id,
		sp_id = sp_id
	}
end

return _M

---@class klib.auth.region_info
---@field nation_id number
---@field state_id number
---@field city_id number
---@field district_id number
---@field sp_id number
