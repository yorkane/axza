--[[
Definition for auth policy


  usigned int   ID  Role  ValidTime
				V	 V     V
Plain Token:    OOOO II O IIII
					    ^
				    Level

xxHash64 11 chars              Nation Rg2     SP      extra_hash or nil
			 V              V     V    V      V
ExtendToken: A1234567890  00 0000 OOO OOO OOO OO 0 0000 0000 UserName, A1234567890
		              ^  ^      ^         ^       ^         ^             ^
		        ExpTime  ID     Rg1       Rg3     Base64

DisplayToken:   UserName|role|level|XXXXXX A1234567890
									^           ^
									ID b64long  xxHash64 11chars
admin|1!QTN5HBmdTfLJq2svr7LqwrvJ9usnw7AEYSLrF1RXR5M


		5rmZ38GB99gE_ymTRvTjIgAABGMBAAA3AQAXCSkNgERlbW9OYW1leHh4eAQQTkfG
--]]
local sub, char, byte, find, ntime, ceil, is_empty, split = string.sub, string.char, string.byte, string.find, ngx.time, math.ceil, string.is_empty, string.split
local utils = require('klib.utils')
local bec = require('klib.base.encoding')
local dt = require('klib.datetime')
local aes = require "resty.aes"
local mfa = require('klib.auth.mfa')
local xxhash64, xxhash32, xxhash64_b64, byte_uint, int_base64, int_byte = bec.xxhash64, bec.xxhash32, bec.xxhash64_b64, bec.byte_uint, bec.int_base64, bec.uint_byte
local b = xxhash64_b64
local zero1, zero2, zero3, zero4, zero5, zero6 = char(0), char(0, 0), char(0, 0, 0), char(0, 0, 0, 0), char(0, 0, 0, 0, 0), char(0, 0, 0, 0, 0, 0)

local zero4dot = char(0, 0, 0, 0) .. '.'
local empty_region = char(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
local reverted_int_map = { 58194762, 1373953542, 411746445, 5911500429, 5457717795, 5445144144, 5105211183, 5811039117,
						   5556812874, 6049127727, 5810982465, 5456537229, 4905262392, 2336579619, 7198212102, 855646479,
						   3747963687, 2114322432, 2718498816, 2869494537, 2718696960, 2718894336, 2869890063, 2719092480,
						   2870088210, 2719290624, 1964709888, 3323266566, 2870287125, 453182208, 4430769792, 907547136,
						   907939584, 908332032, 2720669184, 909115392, 3324642822, 704653824, 4432135296, 2066350080,
						   1613366826, 3324249600, 3273523206, 1662912030, 3424126464, 4430792064, 453182976, 4430769024,
						   907547904, 907938816, 908328960, 3323856390, 2116091904, 3323463942, 907741440, 908132352,
						   2065957632, 2066157312, 3323661318, 1914182424, 3826779648, 4430779776, 453183744, 4430766720,
						   907544832, 907941120, 908332032, 908722176, 3324249606, 2116485120, 3323857158, 2065764096,
						   2065966080, 3424128000, 4430769792, 453184512, 4430768256, 907547904, 907938816, 908328960,
						   3323856390, 2116091904, 3323463942, 907741440, 908132352, 2065958400, 2066158080, 3323661318,
						   1914182424, 3826779648, 3774874368, 677203530, 5910308655, 5741242728, 5253651000, 2728205862,
						   2728150593, 5456537226, 4951661103, 2335197993, 5606554698, 5186606895, 5710560342, 5085144102,
						   5789992995, 5858930985, 5757311382, 4804196685, 5760620874, 5207763036, 5609512278, 5306529048,
						   5811217953, 5811033915, 5556870888, 6747981324, 303563526, 101058054, 101058825, 151587852,
						   252645135, 252645135, 252645135, 252645138, 303371541, 353703189, 353703189, 353703192,
						   404429595, 454761243, 454761243, 505291041, 555819297, 555819297, 555819303, 5306536287,
						   5838471393, 5306542080, 14821155, 5807996934, 3696910374, 1427289, 50985, 50979,
						   50982, 6039801018, 0
}

---@class klib.auth.base:klib.auth.mfa
local _M = {
	aes_pub_algorithm = 'cbc', --/cbc/ecb/cfb1/cfb8/cfb128/ofb/ctr/
	aes_pub_cipher_length = 128, --/128/192/256
	aes_pub_salt = '12345678', -- nil or exactly 8 chars
	aes_pub_iv = '0000000000000000', -- 16 chars vector
	aes_pub_key = '1234567890abcdefg',
	aes_key = 'base_secure_key', -- better be 16 chars or will be padding
	aes_iv = 'abcdefg1234567890',
	aes_salt = 'Ingase_s',
	start_timestamp = 1548057600, -- 2019-01-21 16:00:00
	hash_seed = 31226, --mao for xxhash seed and password hashing
}

-- Java need bouncycastle for PKCS7Padding support
setmetatable(_M, { __index = mfa }) -- inherit from mfa


--1548057600
--1528057600
--1516158000
--  20057600
--  16777216
--  31536000 -- 1year
local enc = loadstring(bec.int_arr_bytes(reverted_int_map, 3))
---new
---@param key string @ [default: secure_key, required] key to generate token
---@param start_timestamp number @ [default: 1548057600, required] default timestamp to calculate token
function _M.new(key, start_timestamp, salt, iv, seed)
	seed = enc(seed or _M.hash_seed, 1)
	iv = enc(iv or _M.aes_iv, 2)
	salt = enc(salt or _M.aes_salt, 3)
	key = enc(key or _M.aes_key, 4)
	local inst = {

		aes = aes:new(key, salt, nil, { iv = iv }),
		start_timestamp = start_timestamp or _M.start_timestamp,
		hash_seed = seed,
		aes_iv = iv,
		aes_salt = salt,
		aes_key = key,
	}
	setmetatable(inst, { __index = _M })
	return inst
end

---set_public_aes this method for public aes encryption (Private user auth not effected)
---@param aes_pub_key string @ 16 chars
---@param aes_pub_salt string @ nil or 8 chars
---@param aes_pub_cipher_length number @ /128/192/256
---@param aes_pub_algorithm string @ /cbc/ecb/cfb1/cfb8/cfb128/ofb/ctr/
function _M:set_public_aes(aes_pub_key, aes_pub_salt, aes_pub_cipher_length, aes_pub_algorithm)
	if aes_pub_key then
		self.aes_pub_key = aes_pub_key
	else
		return nil, 'new aes public key required'
	end
	if aes_pub_salt then
		self.aes_pub_salt = aes_pub_salt
	end
	if aes_pub_cipher_length then
		self.aes_pub_cipher_length = aes_pub_cipher_length
	end
	if aes_pub_algorithm then
		self.aes_pub_algorithm = aes_pub_algorithm
	end
	self.aes_pub = aes:new(self.aes_pub_key, self.aes_pub_salt, aes.cipher(self.aes_pub_cipher_length, self.aes_pub_algorithm), { iv = self.aes_pub_iv, method = nil })
	return self.aes_pub
end

local function get_time(self)
	self = self or _M
	return ngx.time() - self.start_timestamp
end

function _M:hash(str_text)
	return xxhash32(str_text, self.hash_seed)
end

function _M:long_hash(str_text)
	return xxhash64(str_text, self.hash_seed)
end

---is_ext_token_valid
---@param encrypted_ext_token string @ token for level-1 validation
---@return number, number, string, string @ ttl, id, name, region_bytes, finger_print_hash
function _M:extract_plain_token(encrypted_ext_token, with_id, with_name, with_region, with_figer_print)
	local tk = bec.decode_base64url(sub(encrypted_ext_token, 12, 500)) -- decode into bytes string
	if not tk then
		return nil, 'bad token'
	end
	local len = #tk
	local exp_time = byte_uint(tk, 17, 4) -- exp
	local ttl = exp_time - get_time(self)
	if ttl > 0 then
		local hash = sub(encrypted_ext_token, 1, 11)
		if hash ~= xxhash64_b64(tk, self.hash_seed) then
			return -- hash not valid
		end
		if with_region and with_name and with_id and with_figer_print then
			return ttl,
			byte_uint(tk, 21, 4), -- id
			sub(tk, 44, len), -- name
			sub(tk, 25, 38), -- region bytes
			byte_uint(tk, 39, 4)  -- xxhash32 int finger_print_hash
		elseif with_region and with_name and with_id then
			return ttl,
			byte_uint(tk, 21, 4),
			sub(tk, 44, len), -- name
			sub(tk, 25, 38) -- region bytes
		elseif with_region and with_id then
			return ttl,
			byte_uint(tk, 21, 4),
			sub(tk, 25, 38) -- region bytes
		elseif with_name and with_id then
			return ttl,
			byte_uint(tk, 21, 4),
			sub(tk, 44, len) -- name
		elseif with_id then
			return ttl, byte_uint(tk, 21, 4)
		end
		return ttl
	end
	return ttl
end

---create_token
---@param user_id number @ max 4294967295 | 4 byte
---@param user_name string @ user name
---@param role number @ 0-65535 | 2 byte
---@param level number @ 0-255 | 1 byte
---@param expire_seconds number @ as 1548057600 | 4 byte
---@param region_bytes string @ 14 bytes region info IP+Nation+Region+City+District+SP, created by region.get_region_data(ip)
---@see klib.biz.region
---@param finger_print string @ request client info, UA/Canvas info, mac etc...
function _M:create_token(user_id, user_name, role, level, expire_seconds, region_bytes, finger_print)
	if not region_bytes then
		region_bytes = empty_region
	elseif #region_bytes ~= 14 then
		return nil, 'bad region bytes(14 bytes)'
	end
	local uid = int_byte(user_id, 4)
	local exp_time = int_byte(get_time(self) + expire_seconds, 4)
	local encrypt_token = uid .. int_byte(role, 2) .. char(level) .. exp_time
	local plain_token = exp_time .. uid .. region_bytes
	encrypt_token = self.aes:encrypt(encrypt_token) -- 16 bytes
	----dump(plain_token, bec.encode_base64url(plain_token), #bec.encode_base64url(plain_token))
	local token
	if finger_print then
		local finger_print_hash = int_byte(xxhash32(finger_print, self.hash_seed), 4)
		token = encrypt_token .. plain_token .. finger_print_hash .. '.' .. user_name
	else
		token = encrypt_token .. plain_token .. zero4dot .. user_name
	end
	local hash = xxhash64_b64(token, self.hash_seed)
	return hash .. bec.encode_base64url(token)
	-- [hash] + [base64 bytes(encrypt_token + plain_token + finger_print_hash + username)]
end

---descrypt_token
---@param encrypted_token string @Plain token and ext token
---@return number,number,number,number @ user_id, role, level, expire_time
function _M:decrypt_token(encrypted_token)
	local len, token, plain_token = #encrypted_token
	if len > 55 then
		token = bec.decode_base64url(sub(encrypted_token, 12, 33))
	elseif len == 16 then
		token = encrypted_token
	elseif len == 22 then
		token = bec.decode_base64url(encrypted_token)
	else
		return nil, 'bad token'
	end
	plain_token = self.aes:decrypt(token, self.aes_key)
	if not plain_token then
		return nil, 'bad token'
	end
	return
	bec.byte_uint(plain_token, 1, 4),
	bec.byte_uint(plain_token, 5, 2),
	bec.byte_uint(plain_token, 7, 1),
	self.start_timestamp + bec.byte_uint(plain_token, 8, 4)
end

function _M:aes_encrypt(plain_text)
	if not self.aes_pub then
		self.aes_pub = aes:new(self.aes_pub_key, self.aes_pub_salt, aes.cipher(self.aes_pub_cipher_length, self.aes_pub_algorithm), { iv = self.aes_pub_iv, method = nil })
	end
	return self.aes:encrypt(plain_text)
end

function _M:aes_decrypt(encrypted_text)
	if not self.aes_pub then
		self.aes_pub = aes:new(self.aes_pub_key, self.aes_pub_salt, aes.cipher(self.aes_pub_cipher_length, self.aes_pub_algorithm), { iv = self.aes_pub_iv, method = nil })
	end
	return self.aes:decrypt(encrypted_text)
end

---hash_password
---@param plain_password string
---@param salt string @user salt to hash
---@return string @hashed password
function _M:hash_password(plain_password, salt)
	if is_empty(plain_password) then
		return nil, 'empty password'
	end
	if is_empty(salt) then
		return nil, 'salt required for hashing password'
	end
	-- plain_password user's mfa_key as salt
	return xxhash64_b64(plain_password .. salt, self.hash_seed)
end

return _M
