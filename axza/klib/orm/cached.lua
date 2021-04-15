require('klib.common')
local utils = require('klib.utils')
local datetime = require('klib.datetime')
local find, indexOf, is_datetime, sub, split, insert, concat, tonumber, hash, array, copy_to, type, is_empty, assert, tostring = string.find, string.indexOf, datetime.is_datetime_string, string.sub, string.split, table.insert, table.concat, tonumber, table.hash, table.array, table.copy_to, type, string.is_empty, assert, tostring
local next, unpack, pcall, pairs = next, unpack, pcall, pairs
local nfind, nmatch, gmatch, byte, char = ngx.re.find, ngx.re.match, ngx.re.gmatch, string.byte, string.char
local dump, logs, dump_class, dump_lua, dump_doc, dump_dict = require('klib.dump').locally()
local crc32_long, t_empty, ngsub = ngx.crc32_long, table.isempty, ngx.re.gsub
---@type ngx.ipc
local ipc = require('ngx.ipc')

local event_bus = require('klib.biz.event_bus').start()
local ipc_cache = require('klib.biz.ipc_cache')
local base = require('klib.orm.init')
local TAG = 'ORM_CACHED '
local nlog = ngx.log
local INFO = ngx.INFO
local ERR = ngx.ERR

---@class klib.orm.cached:klib.orm.init
---@field cache klib.biz.ipc_cache
---@field event_prefix string @ instance name for ipc event communication prefix
---@field cache_prefix string @ instance name for ipc_cache key prefix
---@field prefix_id string @
---@field prefix_item string @
---@field prefix_ids string @
---@field prefix_list string @
---@field prefix_map string @
---@field prefix_sql string @
---@field prefix_sql_map string @
local _M = {
	expire_policy_conf = nil--{}
}

setmetatable(_M, { __index = base })

---new
---@param dict_key string @sharedDict Key
---@param table_name string
---@param db_name string
---@param mysql_conf mysql.conf
---@return klib.orm.cached, string
function _M.new(dict_key, table_name, db_name, mysql_conf, expire_policy_conf)
	local inst, err = base.new(table_name, db_name, mysql_conf)
	if err then
		return nil, err
	end
	inst.cache = ipc_cache.new(dict_key)
	inst.event_prefix = dict_key .. '.' .. inst._DB .. '.' .. inst._NAME .. '.'
	local cache_prefix = char(1) .. inst._DB .. '.' .. inst._NAME
	inst.cache_prefix = cache_prefix
	inst.prefix_item = cache_prefix .. '$'
	inst.prefix_id = cache_prefix .. '@'
	inst.prefix_ids = cache_prefix .. '/ids/'
	inst.prefix_list = cache_prefix .. '/list/'
	inst.prefix_map = cache_prefix .. '/map/'
	inst.prefix_sql = cache_prefix .. '/sql/list'
	inst.prefix_sql_map = cache_prefix .. '/sql/map/'
	setmetatable(inst, { __index = _M })
	_M.apply_expire_policy(inst, expire_policy_conf)
	return inst
end

function _M.set_default_config(mysql_config)
	base.default_config = mysql_config
	base.default_db = mysql_config.connect_config.database
	return _M
end

---apply_cache_policy
---@param policy_conf table<string, number|boolean|table<string,number|boolean>> @ table name that indicate table or fields need to trigger expire or cache
function _M:apply_expire_policy(policy_conf)
	if not policy_conf then
		return
	end
	policy_conf = policy_conf[self._DB]
	if policy_conf then
		policy_conf = policy_conf[self._NAME]
		if policy_conf then
			self.expire_policy_conf = policy_conf
		end
	end
	self:register_ipc_event()
end

---delete_by_id
---@param id string|number
---@return number @ row deleted
function _M:delete(id)
	local res, err = base.delete(self, id)
	if not err then
		self:expires_by('delete', id)
	end
	return res, err
end

---insert a new record
---@param obj_po table
---@param insert_pk boolean @ insert primary key within po
---@param sql_array string[] @collect insert sql, no insert sql will executed;
function _M:insert(obj_po, insert_pk, sql_array)
	local res, err = base.insert(self, obj_po, insert_pk, sql_array)
	local id = obj_po[self._PK]
	if not err and id then
		self:expires_by('insert', id)
	end
	return res, err
end

---update primary key will not update!
---@param obj_po table
---@param where string|table @ empty where will be ignored
---@param update_pk boolean @force to update primary key
---@param sql_array table @collect update sql, no update sql will executed;
---@return table
function _M:update(obj_po, where, update_pk, sql_array)
	local res, err = base.update(self, obj_po, where, update_pk, sql_array)
	if not err and res.affected_rows > 0 then
		local conf = self.expire_policy_conf
		if conf then
			if type(conf) == 'table' then
				for i, v in pairs(conf) do
					local val = obj_po[i] -- check field
					if val and v then
						self:expires_by('update', obj_po[self._PK] or '-100')
						break
					end
				end
			else
				self:expires_by('update', obj_po[self._PK] or '-100')
			end
		end
	end
	return res, err
end

---batch_update
---@param po_list table[] @plain object list
---@param where string
---@param update_pk boolean
function _M:batch_update(po_list, where, update_pk)
	local res, err, id_arr = base.batch_update(self, po_list, where, update_pk)
	if not err and res.affected_rows > 0 then
		self:expires_by('batch_update', concat(id_arr, ','))
	end
	return res, err
end

---batch_insert
---@param po_list table @po array to insert
---@param insert_pk boolean @ Nullable insert with pk
function _M:batch_insert(po_list, insert_pk, callback)
	local res, err = base.batch_insert(self, po_list, insert_pk, callback)
	if not err and res.affected_rows > 0 then
		self:expires_by('batch_insert', '-100')
	end
	return res, err
end

---update_by_ids
---@param id_arr table @id list array
---@param obj_po table @plain object to indicate which fields to update
---@param sql_array table @return sql command with sql_array injected
function _M:update_by_ids(id_arr, obj_po, sql_array)
	local res, err = base.update_by_ids(self, id_arr, obj_po, sql_array)
	if not err and res.affected_rows > 0 then
		self:expires_by('batch_update', concat(id_arr, ','))
	end
	return res, err
end

--{{{ cache methods

---cache_by_id
---@param cache_seconds number
---@param ignore_lru_cache boolean
---@param id number|string @identifier for this data
---@return table, string, string @ data, key, error
function _M:cache_by_id(cache_seconds, ignore_lru_cache, id)
	if not id then
		return
	end
	local key, str, ok = self.cache_prefix .. '@' .. id
	local po = self.cache:get(key)
	if po then
		if type(po) == 'string' then
			po = self:deserialize(po)
		end
		return po, key
	end
	ok, po = pcall(self.get_by_id, self, id)
	if not ok then
		nlog(ERR, po)
		local err = po
		if find(po, 'API disabled', 1, true) then
			ngx.timer.at(0, _M.cache_by_id, self, cache_seconds, ignore_lru_cache, id)
			po = self.cache:get(key, true, ignore_lru_cache)
			if po then
				return self:deserialize(po), key, err
			end
		end
		return nil, key, err
	end
	if po then
		if ignore_lru_cache then
			str = self:serialize(po)
			self.cache:bucket_set(self.cache_prefix .. '@', id, str, cache_seconds, true)
		else
			self.cache:bucket_set(self.cache_prefix .. '@', id, po, cache_seconds)
		end
		return po, key
	end
end

---cache_by_ids
---@param cache_seconds number
---@param ignore_lru_cache boolean
---@param id_arr_str string
---@param map_key_field string
---@param is_failover boolean @Nullable failover for this cache
---@return table[], string, boolean, string @list, key,is_expired, error
function _M:cache_by_ids(cache_seconds, ignore_lru_cache, id_arr_str, map_key_field, is_failover)
	local prefix = self.cache_prefix .. '/ids/'
	local uid = crc32_long(id_arr_str)
	local key = prefix .. uid
	local list, is_expired = self.cache:get(key, ignore_lru_cache, is_failover)
	if list and not is_expired then
		return list, key
	end
	local ok, res, err = pcall(self.get_by_ids, self, id_arr_str)
	if not ok then
		nlog(ERR, res)
		local err = res
		if find(res, 'API disabled', 1, true) then
			ngx.timer.at(0, _M.cache_by_ids, self, cache_seconds, ignore_lru_cache, id_arr_str, map_key_field, is_failover)
			if not is_failover then
				list, is_expired = self.cache:get(key, true)
				if list then
					return list, key, is_expired, err
				end
			end
		end
		return list, key, is_expired, err
	end
	if not err then
		list = res
		if list and type(list) == 'table' then
			if map_key_field then
				local map = hash(#list)
				for i = 1, #list do
					local it = list[i]
					local ikey = it[map_key_field]
					if not ikey then
						return list, nil, is_expired, 'wrong map_key_field for this model'
					end
					map[ikey] = it
				end
				list = map
			end
			self.cache:bucket_set(prefix, uid, list, cache_seconds, ignore_lru_cache, is_failover)
			return list, key, false
		end
	end
	return list, key, is_expired, res.err
end

---cache_list
---@param cache_seconds number @if hit the cache, will refresh the cache seconds
---@param key string @Nullable, if you have one could speedup the performance
---@param page number @Nullable
---@param page_size number @Nullable
---@param custom_obj table @Nullable
---@param where table|string @Nullable
---@param orderby string @Nullable
---@param is_failover boolean @Nullable failover for this cache
---@return table[], string, boolean, string @ return the list and the new key. data, key, is_expired, error
function _M:cache_list(cache_seconds, key, page, page_size, custom_obj, where, orderby, is_failover)
	local list, is_expired
	if key then
		list, is_expired = self.cache:get(key, cache_seconds, is_failover)
		if list and not is_expired then
			return list, key
		end
	end
	page = page or 1
	page_size = page_size or 200
	local cmd = self:get_list(page, page_size, custom_obj, where, orderby, true)
	local uid = crc32_long(cmd)
	local prefix = self.cache_prefix .. '/list/'
	key = prefix .. uid
	local is_ok, res, err = pcall(self.db.exec, self.db, cmd)
	if not is_ok then
		err = res
		nlog(ERR, list)
		if find(err, 'API disabled', 1, true) then
			if not is_failover then
				-- try to get stale cache
				list, is_expired = self.cache:get(key, true)
				if list then
					return list, key, is_expired, err
				end
			end
			ngx.timer.at(0, _M.cache_list, self, cache_seconds, key, page, page_size, custom_obj, where, orderby, is_failover)
		end
		return list, key, is_expired, err
	end
	if not err and res and type(res) == 'table' then
		self.cache:bucket_set(prefix, uid, res, cache_seconds, false, is_failover)
		nlog(INFO, TAG, key, ' set into cache ', cache_seconds)
		return res, key, false
	end
	return list, key, is_expired, err or 'empty query result'
end

---cache_map
---@param cache_seconds number @if hit the cache, will refresh the cache seconds
---@param key string @Nullable, if you have one could speedup the performance
---@param map_key_field string @Nullable the map key field
---@param page number @Nullable
---@param page_size number @Nullable
---@param custom_obj table @Nullable
---@param where table|string @Nullable
---@param orderby string @Nullable
---@param is_failover boolean @Nullable failover for this cache
---@return table[], string, boolean, string @ return the list and the new key. data, key, is_expired, error
function _M:cache_map(cache_seconds, key, map_key_field, page, page_size, custom_obj, where, orderby, is_failover)
	if not map_key_field then
		return nil, nil, 'empty map_key_field'
	end
	local map, is_expired
	if key then
		map, is_expired = self.cache:get(key, cache_seconds, false, is_failover)
		if map and not is_expired then
			return map, key
		end
	end
	page = page or 1
	page_size = page_size or 200
	local cmd = self:get_list(page, page_size, custom_obj, where, orderby, true)
	local uid = crc32_long(cmd)
	key = self.cache_prefix .. '/map/' .. uid
	local is_ok, list, err = pcall(self.db.exec, self.db, cmd)
	if not is_ok then
		err = list
		nlog(ERR, list)
		if find(list, 'API disabled', 1, true) then
			ngx.timer.at(0, function()
				self:cache_map(cache_seconds, key, map_key_field, page, page_size, custom_obj, where, orderby, is_failover)
			end)
			if not is_failover then
				-- try to get stale cache
				list, is_expired = self.cache:get(key, true)
				if list then
					return list, key, is_expired, err
				end
			end
		end
		return map, key, is_expired, err
	end
	if list and type(list) == 'table' then
		local len, item, ikey = #list
		local nmap = hash(len)
		for i = 1, #list do
			item = list[i]
			ikey = item[map_key_field]
			if ikey then
				nmap[ikey] = item
			end
		end
		if next(nmap) then
			self.cache:bucket_set(self.cache_prefix .. '/map/', uid, nmap, cache_seconds, false, is_failover)
			--logs(map)
			nlog(INFO, TAG, key, ' set into cache ', cache_seconds)
			return nmap, key, false
		else
			return map, key, is_expired, 'map_key_field not match to result'
		end
	end
	return map, key, is_expired, err or 'empty query result'
end

---cache_sql
---@param sql_str string @msyql sql command
---@param cache_seconds number
---@param map_key_field string @turn list into map
---@param is_failover boolean @Nullable failover for this cache
---@return table[], string, boolean, string @ return the list and the new key. data, key, is_expired, error
function _M:cache_sql(cache_seconds, key, map_key_field, sql_str, is_failover)
	local prefix = self.cache_prefix .. '/sql/'
	local uid
	if not key then
		prefix = self.cache_prefix .. '/sql/'
		uid = crc32_long(sql_str)
		if map_key_field then
			prefix = prefix .. 'map/'
		else
			prefix = prefix .. 'list/'
		end
		key = prefix .. uid
	end
	local map, is_expired
	map = self.cache:get(key, cache_seconds, false, true)
	if map and not is_expired then
		return map, key
	end

	local is_ok, list, err = pcall(self.db.exec, self.db, sql_str)
	if not is_ok then
		err = list
		nlog(ERR, err)
		if find(list, 'API disabled', 1, true) then
			ngx.timer.at(0, function()
				self:cache_sql(cache_seconds, key, map_key_field, sql_str, is_failover)
			end)
		end
		if not is_failover then
			local nmap, n_is_expired = self.cache:get(key, true)
			if nmap then
				return nmap, key, n_is_expired, err
			end
		end
		return map, key, is_expired, err
	end
	if not list or type(list) ~= 'table' then
		return map, key, is_expired, err or 'empty query result'
	end
	cache_seconds = cache_seconds or 300
	if map_key_field then
		local len, item, ikey = #list
		local nmap = hash(len)
		for i = 1, #list do
			item = list[i]
			ikey = item[map_key_field]
			if ikey then
				nmap[ikey] = item
			end
		end
		if next(nmap) then
			self.cache:bucket_set(prefix, uid, nmap, cache_seconds, false, is_failover)
			nlog(INFO, TAG, key, ' set into cache ', cache_seconds)
			return nmap, key, false
		else
			return map, key, is_expired, 'map_key_field not match any SQL field to result'
		end
	else
		nlog(INFO, TAG, key, ' set into cache ', cache_seconds)
		self.cache:bucket_set(prefix, uid, list, cache_seconds, false, is_failover)
		return list, key, false
	end
end

--}}}

function _M:expires_batch(id)
	local cache = self.cache
	cache:bucket_delete(self.prefix_ids)
	cache:bucket_delete(self.prefix_list)
	cache:bucket_delete(self.prefix_map)
	cache:bucket_delete(self.prefix_sql)
	cache:bucket_delete(self.prefix_sql_map)
	if id then
		cache:bucket_delete(self.prefix_item .. id)
		cache:bucket_delete(self.prefix_id, tonumber(id))
	end
end

function _M:expires_by(event_name, data)
	if self.expire_policy_conf then
		event_bus.emit_local(self.event_prefix .. event_name, data or '-100')
		event_bus.emit(self.event_prefix .. event_name, data or '-100')
	end
end

---register_ipc_event this method will automatically called after apply_cache_policy(). call it while losing ipc events
function _M:register_ipc_event()
	if not self.expire_policy_conf then
		return nil, 'expire_policy_conf not defined, please `apply_cache_policy()` first'
	end
	local cache = self.cache
	event_bus.on(self.event_prefix .. 'update', function(id)
		self:expires_batch(id)
	end)

	event_bus.on(self.event_prefix .. 'batch_update', function(id_arr_str)
		self:expires_batch()
		if id_arr_str and #id_arr_str > 1 then
			local arr = split(id_arr_str, ',')
			if arr then
				for i = 1, #arr do
					cache:bucket_delete(self.prefix_item .. arr[i])
					cache:bucket_delete(self.prefix_id, tonumber(arr[i]))
				end
			end
		end
	end)

	event_bus.on(self.event_prefix .. 'insert', function(id)
		self:expires_batch()
	end)

	event_bus.on(self.event_prefix .. 'batch_insert', function()
		self:expires_batch()
	end)

	event_bus.on(self.event_prefix .. 'delete', function(id)
		self:expires_batch(id)
		--logs('delete', event_bus.name, event_bus.is_first, table_name, id, '========>')
	end)

	event_bus.on(self.event_prefix .. 'batch_delete', function(id_arr_str)
		self:expires_batch()
		if id_arr_str and #id_arr_str > 1 then
			local arr = split(id_arr_str, ',')
			if arr then
				for i = 1, #arr do
					cache:bucket_delete(self.prefix_item .. arr[i])
					cache:bucket_delete(self.prefix_id, tonumber(arr[i]))
				end
			end
		end
	end)
end

return _M