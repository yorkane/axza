local sgsub, sfind, sub, char, byte = string.gsub, string.find, string.sub, string.char, string.byte
local tinsert = table.insert
local type = type
local ipairs = ipairs
local pairs = pairs
local mysql = require("resty.mysql")
local resty_lock = require('resty.lock')
local json = require('lib.json')
local nfind, nmatch = ngx.re.find, ngx.re.match
local lid = ngx.worker.id() or '0' .. '/' .. ngx.worker.count() or '0'
local dict = next(ngx.shared)
local IS_DB_DOWN = 'IS_DB_DOWN'
local err_head = char(0, 255, 0)
if dict then
	dict = ngx.shared[dict]
else
	dict = {
		DB_IS_DOWN = false,
		set = function()
		end,
		get = function()
			return false
		end
	}
end

---@class DB
---@field conf mysql.conf
local DB = {
}
local function table_is_array(t)
	if type(t) ~= "table" then
		return false
	end
	local i = 0
	for _ in pairs(t) do
		i = i + 1
		if t[i] == nil then
			return false
		end
	end
	return true
end
local function del_userdata(item)
	for i, v in pairs(item) do
		if type(v) == 'userdata' then
			item[i] = nil
		end
	end
	return item
end
local function remove_userdata(res)
	local tp = type(res)
	if tp == 'table' then
		if #res > 0 then
			for i = 1, #res do
				del_userdata(res[i])
			end
		else
			del_userdata(res)
		end
	end
	return res
end

---new
---@param conf appconf @configurations
---@param tag string @ tag name to log
---@return DB @new database object
function DB:new(conf, tag)
	tag = tag or '[SQL]'
	local instance = {
		conf = conf,
		tag = tag .. lid
	}
	setmetatable(instance, { __index = self })
	return instance
end

function DB:get_lock(sql)
	local rc, lock_seconds, lock_key = self.conf.reduce_cache, 0
	if not rc or not rc.enable then
		return
	end
	local mc = nmatch(sql, [[^select .+?from[^\w]+(\w+)[^\w]+]], 'jio')
	if not mc then
		return
	end
	local name = mc[1]
	lock_seconds = rc.tables[name]
	if not lock_seconds or lock_seconds < 1 then
		return -- if no lock seconds, means the reduce_cache disabled
	end
	if lock_seconds > rc.max_seconds then
		lock_seconds = rc.max_seconds -- could not exceed the max lock seconds
	end
	lock_key = '/lock/sql_cmd/' .. ngx.crc32_long(sql) -- build lock key based on sql content. In case the same sql will hit the same lock
	local cache_key = '#' .. name .. lock_key -- build cache key based ony lock key
	local data = dict:get(cache_key)
	if data then
		data = json.decode(data)
		if data then
			return nil, data, cache_key -- Hit the cache, get the data, and return the cache_key
		end
	end
	local lock, err = resty_lock:new('cache', { timeout = lock_seconds, exptime = rc.max_seconds }) -- New instance resty.lock every time
	if not lock then
		return -- require lock failed, then reduce cache will not work
	end
	--logs(lock_seconds .. ' seconds start to get locker: ' .. lock_key)
	local elapsed, err = lock:lock(lock_key) -- If lock already set, this will hold current request. Once lock required, current thread will resume , but NOT hold the process(none-blocking)
	--if elapsed > 1 then
	--end
	data = dict:get(cache_key) -- try to get cache again, might be cache was warm while waiting for lock
	if data then
		data = json.decode(data)
		if data then
			lock:unlock() -- release lock immediately
			return nil, data, cache_key
		end
	end
	if not err then
		ngx.log(ngx.INFO, 'locker getted: ', lock_key, 'wait for ', elapsed)
		return lock, nil, cache_key, lock_seconds + 1
	end
end

---exec
---@param sql string @SQL command to execute
---@return table, string, number, number @table, err, errno, sqlstate
function DB:exec(sql)
	if not sql then
		ngx.log(ngx.WARN, self.tag, "sql parse error! please check")
		return nil, "sql parse error! please check"
	end
	local locker, data, cache_key, cache_seconds = self:get_lock(sql)
	if data then
		if type(data) == 'string' and sub(data, 1, 3) == err_head then
			ngx.log(ngx.INFO, 'database reduce cache errors reuse: ' .. cache_key)
			return nil, data
		end
		ngx.log(ngx.INFO, 'database reduce cache hit: ' .. cache_key)
		return data
	end

	local tag = self.tag
	local conf = self.conf
	--if dict:get(IS_DB_DOWN .. conf.connect_config.database) then
	--	return nil, "database is shutting down"
	--end
	local start_time = os.clock()
	local db, err = mysql:new()
	if not db then
		local msg = "failed to instantiate mysql: " .. err
		ngx.log(ngx.CRIT, tag, msg)
		return nil, msg
	end
	db:set_timeout(conf.timeout) -- 1 sec
	local ok, err, errno, sqlstate = db:connect(conf.connect_config)
	if not ok then
		local msg = "failed to connect: " .. conf.connect_config.host .. ' err: ' .. err .. ": " .. (errno or '') .. " " .. (sqlstate or '')
		ngx.log(ngx.CRIT, tag, msg)
		-- bring down Dataserver for 60 secs
		dict:set(IS_DB_DOWN .. conf.connect_config.database, true, 60)
		if locker then
			locker:unlock()
		end
		return nil, msg
	end

	db:query("SET NAMES utf8")
	local res, query_err, errcode, sqlstate, is_bad = db:query(sql)
	if not res then
		-- ignore duplicate insert error
		if not sfind(query_err, 'Duplicate entry', 1, true) then
			if #sql > 400 then
				sql = sub(sql, 1, 400) .. '<<<<<<'
			end
			ngx.log(ngx.CRIT, tag, 'query failed :', res, query_err, ": ", errcode, ": ", sqlstate, "\n", sql, ' -- duration:', math.ceil((os.clock() - start_time) * 1000), ' ms [SQL]\n', debug.traceback('debug', 2))
			is_bad = true
		else
		end
	end
	local res_arr = { remove_userdata(res) };
	local i = 2
	while err == "again" do
		res, err, errcode, sqlstate = db:read_result()
		if not res then
			ngx.log(ngx.CRIT, tag, "bad result #", i, ": ", err, ": ", errcode, ": ", sqlstate, ".")
			break
		else
			tinsert(res_arr, remove_userdata(res))
		end
		i = i + 1
	end
	if not is_bad then
		if #sql > 300 then
			sql = sub(sql, 1, 300) .. '<<<<<<'
		end
		ngx.log(ngx.NOTICE, tag, "\n", sql, ' -- duration:', math.ceil((os.clock() - start_time) * 1000), ' ms [SQL]\n, reused_times:', db:get_reused_times())
	end

	local ok, err_msg = db:set_keepalive(conf.pool_config.max_idle_timeout, conf.pool_config.pool_size)
	if not ok then
		ngx.log(ngx.CRIT, tag, "failed to set keepalive: ", err_msg)
	end

	if #res_arr > 1 then
		res = res_arr
	else
	end
	if cache_key then
		if is_bad then
			dict:set(cache_key, err_head .. query_err, cache_seconds) -- cache error info
		else
			local data = json.encode(res)
			dict:set(cache_key, data, cache_seconds) -- lock second + 1
			ngx.log(ngx.NOTICE, 'locker released: ', cache_key, ' set into cache with size: ', #data)
		end
		locker:unlock()
	end
	return res, query_err, errcode, sqlstate
end

---query
---@param sql string @ sql prepared
---@param params table @ param array
function DB:query(sql, params)
	sql = self:parse_sql(sql, params)
	return self:exec(sql)
end

function DB:select(sql, params)
	return self:query(sql, params)
end

function DB:insert(sql, params)
	local res, err, errno, sqlstate = self:query(sql, params)
	if res and not err then
		return res.insert_id, err
	else
		return res, err
	end
end

function DB:update(sql, params)
	return self:query(sql, params)
end

function DB:delete(sql, params)
	local res, err, errno, sqlstate = self:query(sql, params)
	if res and not err then
		return res.affected_rows, err
	else
		return res, err
	end
end

local function split(str, delimiter)
	if str == nil or str == '' or delimiter == nil then
		return nil
	end

	local result = {}
	for match in (str .. delimiter):gmatch("(.-)" .. delimiter) do
		tinsert(result, match)
	end
	return result
end

local function compose(t, params)
	if t == nil or params == nil or type(t) ~= "table" or type(params) ~= "table" or #t ~= #params + 1 or #t == 0 then
		return nil
	else
		local result = t[1]
		for i = 1, #params do
			result = result .. params[i] .. t[i + 1]
		end
		return result
	end
end

function DB:parse_sql(sql, params)
	if not params or not table_is_array(params) or #params == 0 then
		return sql
	end

	local new_params = {}
	for i, v in ipairs(params) do
		if v and type(v) == "string" then
			v = ngx.quote_sql_str(v)
		end

		tinsert(new_params, v)
	end

	local t = split(sql, "?")
	local sql = compose(t, new_params)
	return sql
end

return DB
