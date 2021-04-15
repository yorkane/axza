--https://github.com/anjia0532/lua-resty-redis-util -- Copyright (C) Anjia (anjia0532)
--https://github.com/ledgetech/lua-resty-redis-connector

local redis_c = require("resty.redis.connector")
local _, new_tab = pcall(require, "table.new")
local sleep = ngx.sleep

local nfind, ngx_log, debug = ngx.re.find, ngx.log

local sfind, type, rawget, ipairs, tonumber, time = string.find, type, rawget, ipairs, tonumber, ngx.time
local clone = require('table.clone')

local dict = ngx.shared['cache'] or ngx.shared[next(ngx.shared)]
local unsupport_method = { subscribe = 1, eval = 1 }
local failover_redis = {
    set = function(self, key, val, EX, ttl, NX)
        return dict:set(key, val, ttl)
    end,
    setex = function(self, key, ttl, val)
        return dict:set(key, val, ttl)
    end,
    setnx = function(self, key, val, ttl)
        return dict:add(key, val, ttl)
    end,
    del = function(self, key)
        return dict:delete(key)
    end,
    get = function(self, key)
        return dict:get(key)
    end,
    decr = function(self, key, num)
        return dict:incr(key, num * -1)
    end,
    ttl = function(self, key)
        local t = dict:ttl(key)
        if t == -1 then
            return 0
        end
        return t
    end
}
local function fail_fun()
    return nil, 'failover_redis ngx.shared.DICT not support instead of Redis'
end
setmetatable(failover_redis, {
    __index = function(self, key, v_key, ...)
        if unsupport_method[key] then
            return fail_fun
        end
        return dict[key] or fail_fun
    end, })

---@class klib.biz.redispool.options
---@field url string @ DSN url, the first priority as config input, will override other settings
local default_options = {
    connect_timeout = 200,
    read_timeout = 1000,
    connection_options = {}, -- pool, etc
    keepalive_timeout = 60000,
    keepalive_poolsize = 30,
    host = "127.0.0.1",
    port = 6379,
    path = "", -- /tmp/redis.sock
    password = "", -- could be sentinel password
    db = 0,
    url = nil, -- DSN url
    master_name = "mymaster",
    role = "master", -- master | slave
    sentinels = {
        -- { host = "127.0.0.1", port = 26379 },
    },
    connection_is_proxied = false,

    disabled_commands = {},
}

---@class klib.biz.redispool:resty.redis
---@field options klib.biz.redispool.options
---@field slave_options klib.biz.redispool.options
local _M = {
    options = default_options,
    slave_options = {},
    subscribes = {},
    _version = 0.7,
    _conf = {},
    allow_failover = false,
    is_exiting = false,
}

local mt = { __index = _M }

local MAX_PORT = 65535



-- if res is ngx.null or nil or type(res) is table and all value is ngx.null return true else false
local function _is_null(res)
    if res == ngx.null or res == nil then
        return true
    elseif type(res) == "table" then
        for _, v in pairs(res) do
            if v ~= ngx.null then
                return false
            end
        end
        -- thanks for https://github.com/anjia0532/lua-resty-redis-util/issues/3
        return true
    end
    return false
end

local function wrap_return(cmd, result, must_wrap)
    if _is_null(result) then
        return
    end
    if must_wrap or sfind(cmd, 'set', 1, true) or sfind(cmd, 'add', 1, true) then
        if result == 1 or result == 'OK' then
            result = true
        elseif result == 0 then
            result = false
        else
        end
    end
    return result
end

local function _debug_err(msg, err)
    ngx_log(ngx.INFO, msg, err)
end

local reg = [[(set|add|insert|push|pub|incr|decr|eval|script|del)]]
local function is_write_cmd(cmd)
    return nfind(cmd, reg, 'jo')
end

local sub_failed = 0
local function read_subscribe_reply(is_premature, self, channel_key, max_wating_seconds, func, param1, param2, param3, param4, param5)
    if is_premature or _M.is_exiting or self.stopped then
        return
    end
    max_wating_seconds = max_wating_seconds or -1
    local red, err = self:fetch()
    if err then
        func(nil, err)
        sub_failed = sub_failed + 1
        if sub_failed > 10 then
            return
        end
    end
    if not err then
        local last_second = time()
        local ok, err = red:subscribe(channel_key)
        if not ok then
            func(nil, err)
            sub_failed = sub_failed + 1
            if sub_failed > 10 then
                return
            end
        end
        if not err then
            if not self.subscribes[channel_key] then
                return -- channel cancelled
            end
            local res, errs = red:read_reply()
            if errs then
                local secs = time() - last_second
                if max_wating_seconds > 0 and secs >= max_wating_seconds then
                    func(nil, errs)
                end
                sub_failed = sub_failed + 1
                if sub_failed > 10 then
                    return
                end
                sub_failed = 0
            else
                if res[1] == "message" then
                    func(res[3], param1, param2, param3, param4, param5)
                    last_second = time()
                end
            end
            red:set_keepalive(100, 20)
        end
    end
    ngx.timer.at((sub_failed * 50) + 0.001, read_subscribe_reply, self, channel_key, max_wating_seconds, func, param1, param2, param3, param4, param5)
end

function _M:stop()
    self.stopped = true
end

-- encapsulation subscribe
---subscribe
---@param channel_key string
---@param max_wating_seconds number @[Nullable] leave -1 or nil to endless running unless unsubscribe
---@param func fun(sub_msg:string, param1, param2, param3, param4, param5):boolean @ return true to cancel receiving
---@param max_wating_seconds number @ end receive after no message for N seconds
---@return string @ errors
function _M:subscribe(channel_key, max_wating_seconds, func, param1, param2, param3, param4, param5)
    self.subscribes[channel_key] = true
    ngx.timer.at(0, read_subscribe_reply, self, channel_key, max_wating_seconds, func, param1, param2, param3, param4, param5)
    return true
end

---unsubscribe stop all running subscribe task binding to channel)key
---@param channel_key string
function _M:unsubscribe(channel_key)
    self.subscribes[channel_key] = false
end

-- init pipeline,default cmds num is 4
---init_pipeline must paired to commit_pipline and no IO operations interrupt. Otherwise cause threading fusion problems
---@param n number @[Nullable] estimate command count
function _M:init_pipeline(n)
    self._reqs = nil
    self._reqs = new_tab(n or 4, 0)
end

-- cancel pipeline
function _M:cancel_pipeline()
    self._reqs = nil
end

-- commit pipeline
function _M:commit_pipeline()
    -- get cache cmds
    local _reqs = rawget(self, "_reqs")
    if not _reqs then
        _debug_err("failed to commit pipeline,reason:no pipeline")
        return nil, "no pipeline"
    end

    self._reqs = nil

    -- init redis
    local redis, err = self:fetch(true)
    if not redis then
        _debug_err("failed to init redis,reason::", err)
        return nil, err
    end
    redis:init_pipeline()
    --redis command like set/get ...
    for _, vals in ipairs(_reqs) do
        -- vals[1] is redis cmd
        local fun = redis[vals[1]]
        -- get params without cmd
        table.remove(vals, 1)
        -- invoke redis cmd
        fun(redis, unpack(vals))
    end

    -- commit pipeline
    local results, err = redis:commit_pipeline()
    if not results or err then
        _debug_err("failed to commit pipeline,reason:", err)
        return {}, err
    end
    -- check null
    if _is_null(results) then
        results = {}
        ngx.log(ngx.WARN, "redis result is null")
    end
    -- put it into the connection pool
    self:release(redis)
    -- if null set default value nil
    for i, value in ipairs(results) do
        if _is_null(value) then
            results[i] = nil
        end
    end

    return results, err
end

-- common method
local function do_command(self, cmd, ...)
    -- pipeline reqs
    local _reqs = rawget(self, "_reqs")
    if _reqs then
        -- append reqs
        _reqs[#_reqs + 1] = { cmd, ... }
        return
    end
    -- init redis
    local is_write = is_write_cmd(cmd)

    local redis, err = self:fetch(is_write)
    if not redis then
        self:release(redis)
        _debug_err("failed to init redis,reason::", err)
        return nil, err
    end
    -- exec redis cmd
    local method = redis[cmd]
    local arg = { ... }
    local result, err = method(redis, ...)
    if not result or err then
        _debug_err("failed to run redis cmd:", cmd, err)
        return nil, err
    end
    if result then
        result = wrap_return(cmd, result, is_write)
    end
    self:release(redis)
    return result
end

--function _M:get_redis(conf, allow_sharedDICT_failover_cache)
--    if self._instance then
--        return self._instance
--    end
--    local err
--    local tp = type(conf)
--    if tp == 'string' then
--        if not sfind(conf, 'redis://', 1, true) or not sfind(conf, 'sentinel://', 1, true) then
--            return nil, 'must be a valid `redis://xxx.xx.xxx:6379/1`  or `sentinel://xxx.xxx.xxx:26379` string'
--        end
--    elseif tp == 'table' then
--        if conf.redis_url or conf.host or conf.sentinels then
--        else
--            return nil, 'not a valid redis conf, `redis_url` field or redis server info must be included'
--        end
--    end
--    self._conf = conf
--    self._instance, err = _M.new(conf, allow_sharedDICT_failover_cache)
--    return self._instance, err
--end

---fetch
---@return resty.redis
function _M:fetch(is_write_operation)
    local opt
    if is_write_operation then
        opt = self.options
    else
        opt = self.slave_options
    end
    local redis, err, inst = redis_c.new(opt)
    if not redis then
        if self.allow_failover then
            return failover_redis
        end
        ngx.log(ngx.NOTICE, "failed to init redis,reason::", err)
        return nil, err
    end
    if opt.sentinels and opt.sentinels[1] then
        inst, err = redis:connect_via_sentinel(opt)
        if err then
            inst, err = redis:connect_via_sentinel(self.options) --slave may not work
            if err then
                inst, err = redis:connect_to_host(opt) -- May connect to the wrong redis server in Local 127.0.0.1:6379
            end
        end
    else
        inst, err = redis:connect_to_host(opt)
        if err then
            inst, err = redis:connect()
        end
    end
    if not inst or err then
        self:refresh_config() -- connection failed, try refresh linked configuration for new redis address
        ngx.log(ngx.NOTICE, "Redis connection not finished,reason::", err)
        if self.allow_failover then
            return failover_redis
        end
        return nil, err
    end
    return inst, nil
end

---setnx Set value while Not Exist key with expire time
---@param key string
---@param val string|number|boolean
---@param ttl number @time to live seconds
---@return boolean, string @success, error
function _M:setnx(key, val, ttl)
    local redis, err = self:fetch(true)
    if err then
        return nil, err
    end
    local res, err

    if ttl and ttl > 0 then
        res, err = redis:set(key, val, 'EX', ttl, 'NX')
        res = wrap_return('set', res)
    else
        res, err = redis:setnx(key, val)
        res = wrap_return('set', res)
    end
    if not res and not err then
        err = 'key exists'
    end
    self:release(redis)
    return res, err
end

local lock_prefix = '__RED_POOL_LOCK'
---lock_key
---@param key string @unique key
---@param max_wait_secs number @[Default 2] max seconds lock to expire
---@return boolean, string@ success, error message
function _M:lock_key(key, max_wait_secs)
    max_wait_secs = max_wait_secs or 2
    if max_wait_secs > 10 or max_wait_secs < 0.4 then
        return false, 'max wait seconds must less than 10 seconds, and greater the 0.4 seconds'
    end
    local lkey = lock_prefix .. key
    local utc = time()
    local ok, err = self:setnx(lkey, utc, max_wait_secs)
    if err then
        return false, err
    end
    if ok then
        return true
    end
    local len = max_wait_secs * 5
    for i = 1, len do
        sleep(0.2)
        ok, err = self:setnx(lkey, utc, max_wait_secs)
        if ok then
            return true
        end
        if err then
            return false, err
        end
    end
    local utc1, err = self:get(key)
    if err then
        return false, err
    end
    -- remove bad lock keys
    if utc - utc1 > 10 then
        ok, err = self:setex(lkey, max_wait_secs, utc)
        return ok, err
    end
end

---unlock_key instantly remove locked key
---@param key string
function _M:unlock_key(key)
    local lkey = lock_prefix .. key
    return self:del(lkey)
end

---scan
---@param key_pattern string @[Required] link xxxx*
---@param func fun(key:string, val:string|number, cursor_index:number, ...):boolean @ return true to break the scan
---@return
function _M:scan(key_pattern, func, ...)
    local count = 20
    local len = 20
    local cursor = 0
    local inx = 0
    local redis = self:fetch()
    while (len == count) do
        local res = redis:scan(cursor, "MATCH", key_pattern, "count", count)
        len = tonumber(res[1]) -- get current scan length
        local list = res[2]
        if #list == 0 then
            return
        end
        cursor = len + cursor
        local vlist = redis:mget(unpack(list)) -- inject matched keys
        if vlist == true or vlist == nil then
            return
        end
        for i = 1, #vlist do
            inx = inx + 1
            local to_end = func(list[i], vlist[i], inx, ...) -- callback each key, val, index
            if to_end then
                break
            end
        end
    end
end

---get_keys
---@param key_pattern string
---@param max_count number
function _M:get_keys(key_pattern, max_count)
    local count = 20
    local len = 20
    local cursor = 0
    local inx = 0
    local key_list = {}
    local redis = self:fetch()
    while (len == count) do
        local res = redis:scan(cursor, "MATCH", key_pattern, "count", count)
        len = tonumber(res[1]) -- get current scan length
        local list = res[2]
        if #list == 0 then
            break
        end
        for i = 1, #list do
            inx = inx + 1
            if inx == max_count then
                break
            end
            key_list[inx] = list[i]
        end
        cursor = len + cursor
    end
    return key_list
end

local bad_redis_url_error_msg = 'must be a valid `redis://PASSWORD@HOST:PORT/DB`  or `sentinel://xxx.xxx.xxx:26379` string'
local bad_redis_option_error_msg = 'not a valid redis conf, `redis_url` field or redis server info must be included'
---@param opts klib.biz.redispool.options
---@return klib.biz.redispool
function _M.new(opts, allow_sharedDICT_failover_cache)
    local options, _local_conf, err
    local tp = type(opts)
    if tp == "string" then
        if not sfind(opts, 'redis://', 1, true) and not sfind(opts, 'sentinel://', 1, true) then
            return nil, bad_redis_url_error_msg
        end
        options = { url = opts }
        options, err = redis_c.parse_dsn(options)
        if not options then
            return nil, err
        end
    elseif tp ~= 'table' then
        return nil, bad_redis_url_error_msg
    else
        if opts.redis_url then
            if not sfind(opts.redis_url, 'redis://', 1, true) and not sfind(opts.redis_url, 'sentinel://', 1, true) then
                return nil, bad_redis_url_error_msg
            end
            options = { url = opts.redis_url }
            options = redis_c.parse_dsn(options)
            _local_conf = opts -- keep the config reference, the config redis_url field may changed
        elseif type(opts.redis) == 'table' then
            if opts.redis.host or opts.redis.sentinels or opts.redis then
            else
                return nil, bad_redis_option_error_msg
            end
            _local_conf = opts -- keep the config reference, the redis config may changed
            options = opts.redis
        elseif opts.host or opts.sentinels then
            options = clone(opts)
        elseif opts.url then
            options = redis_c.parse_dsn(opts)
        else
            return nil, bad_redis_option_error_msg
        end
    end
    options.url = nil -- prevent dsn parse again
    local inst = { _conf = _local_conf or false } -- key config reference
    for key, value in pairs(default_options) do
        if not options[key] then
            options[key] = value
        end
    end
    for k, v in pairs(options) do
        if k == "host" then
            if type(v) ~= "string" then
                return nil, '"host" must be a string'
            end
        elseif k == "port" then
            v = tonumber(v) or 0
            if v < 1 then
                return nil, '"port" must be a valid number'
            end
            if v < 0 or v > MAX_PORT then
                return nil, ('"port" out of range 0~%s'):format(MAX_PORT)
            end
            options.port = v
        elseif k == "password" then
            if type(v) ~= "string" then
                return nil, '"password" must be a string'
            end
        elseif k == "db" then
            v = tonumber(v)
            if not v then
                return nil, '"db_index" must be a number'
            end
            if v < 0 then
                return nil, '"db_index" must be >= 0'
            end
            options.db = v
        elseif k == "keepalive_timeout" then
            if type(v) ~= "number" or v < 0 then
                return nil, 'invalid "timeout"'
            end
        elseif k == "keepalive_poolsize" then
            if type(v) ~= "number" or v < 0 then
                return nil, 'invalid "pool_size"'
            end
        end
    end
    inst.allow_failover = allow_sharedDICT_failover_cache
    inst.options = options
    inst.slave_options = clone(options)
    inst.slave_options.role = 'slave'
    inst = setmetatable(inst, mt)
    return inst
end

---refresh_config the referred configuration may changed, try to apply modification for next redis requests
function _M:refresh_config()
    if not self._conf then
        return
    end
    local options
    if self._conf.redis_url then
        options = redis_c.parse_dsn({ url = self._conf.redis_url })
    elseif self._conf.redis then
        options = self._conf.redis -- the reference keeps redis config
    end
    self.options = options
    self.slave_options = clone(options)
    self.slave_options.role = 'slave'
end

---- dynamic cmd
setmetatable(_M, { __index = function(self, cmd)
    local method = function(self, ...)
        return do_command(self, cmd, ...)
    end

    -- cache the lazily generated method in our
    -- module table
    _M[cmd] = method
    return method
end })

---release
---@param redis resty.redis
function _M:release(redis)
    if redis and redis.set_keepalive then
        redis:set_keepalive(self.options.keepalive_timeout, self.options.keepalive_poolsize)
    end
end

function _M.test()
    require('klib.dump').global()
    --local red, err = _M.new('redis://PASSWORD@127.0.0.1:6379/2')
    --local options = red.options
    --assert(options.host == '127.0.0.1')
    --assert(options.port == 6379)
    --assert(options.password == 'PASSWORD')
    --assert(options.db == 2)
    --assert(options.role == 'master')
    --assert(options.master_name == 'mymaster')
    --assert(options.url == nil)
    --
    --local opt = clone(default_options)
    ----opt.sentinels = { { host = "127.0.0.1", port = 26379 }, { host = "127.0.0.1", port = 26380 } }
    --red = _M.new(opt)
    --
    --opt = red.slave_options
    --assert(opt.host == '127.0.0.1')
    --assert(opt.port == 6379)
    --assert(opt.db == 0)
    --assert(opt.role == 'slave')
    --assert(opt.url == nil)
    local function _t(redis_opt)
        local red, err = _M.new(redis_opt)
        assert(red, err)
        --dump(red)
        local key = '__redis_pool_test' .. ngx.localtime()
        local ok, err = red:setnx(key, 'test1', 1)
        local ok, err = red:setnx(key, 'test1', 1)
        assert(not ok and err, 'should not be set')
        local res, err = red:get(key)
        --dump(red:get(key))
        assert(res == 'test1', err)
        res, err = red:del(key)
        assert(res, err)
    end
    local url = 'redis://pika3@192.168.1.2:9222'
    --_t(url)
    _t({ redis_url = url })
    --_t({ connect_timeout = 2000, url = url })
    --_t({ redis = { host = '192.168.1.2' } })
end

return _M

---@class resty.redis
---@field _VERSION string
---@field array_to_hash fun (self:resty.redis, array:table[]):table<string, string> @ Auxiliary function that converts an array-like Lua table into a hash-like table.
---@field auth fun (self:resty.redis, password:string) @ Redis uses the AUTH command to do authentication: http://redis.io/commands/auth
---@field close fun (self:resty.redis)  --/usr/local/openresty/lualib/resty/redis.lua:115
---@field connect fun (self:resty.redis, host:string, port:number, options_table:table)  @ redis:connect("unix:/path/to/unix.sock", options_table?)
---@field decr fun (self:resty.redis, key:string, ...)
---@field del fun (self:resty.redis, key:string, ...)
---@field eval fun (self:resty.redis, key:string, ...)
---@field expire fun (self:resty.redis, key:string, ...)
---@field get fun (self:resty.redis, key:string, ...)
---@field get_reused_times fun (self:resty.redis)  @--/usr/local/openresty/lualib/resty/redis.lua:105
---@field hdel fun (self:resty.redis, hash_name:string, key:string, ...)
---@field hexists fun (self:resty.redis, hash_name:string, key:string, ...)
---@field hget fun (self:resty.redis, hash_name:string, key:string, ...)
---@field hkeys fun (self:resty.redis, hash_name:string, key:string, ...)
---@field hmget fun (self:resty.redis, hash_name:string, key:string, ...)
---@field hmset fun (self:resty.redis, hash_name:string, key:string, ...)  @--/usr/local/openresty/lualib/resty/redis.lua:324
---@field hset fun (self:resty.redis, hash_name:string, key:string, ...)
---@field incr fun (self:resty.redis, key:string, ...)
---@field lindex fun (self:resty.redis, key:string, ...)
---@field linsert fun (self:resty.redis, key:string, ...)
---@field llen fun (self:resty.redis, key:string, ...)
---@field lpop fun (self:resty.redis, key:string, ...)
---@field lpush fun (self:resty.redis, key:string, ...)
---@field keys fun (self:resty.redis, key_pattern:string):string[]
---@field rpop fun (self:resty.redis, key:string, ...)
---@field rpush fun (self:resty.redis, key:string, ...)
---@field lrange fun (self:resty.redis, key:string, ...)
---@field mget fun (self:resty.redis, key:string, ...)
---@field mset fun (self:resty.redis, key:string, ...)
---@field pttl fun (self:resty.redis, key:string, ...) @ --return time to live seconds of  the key
---@field psubscribe fun (self:resty.redis, key:string, ...)  @--/usr/local/openresty/lualib/resty/redis.lua:305
---@field punsubscribe fun (self:resty.redis, key:string, ...)  @--/usr/local/openresty/lualib/resty/redis.lua:316
---@field read_reply fun (self:resty.redis)  @--/usr/local/openresty/lualib/resty/redis.lua:274
---@field sadd fun (self:resty.redis, key:string, ...)
---@field script fun (self:resty.redis, key:string, ...)
---@field sdiff fun (self:resty.redis, key:string, ...)
---@field set fun (self:resty.redis, key:string, ...)
---@field setex fun (self:resty.redis, key:string, ttl, val)
---@field set_keepalive fun (self:resty.redis, max_idle_timeout:number, pool_size:number)
---@field set_timeout fun (self:resty.redis, timeout:number) @ Sets the timeout (in ms) protection for subsequent operations, including the connect method.
---@field sinter fun (self:resty.redis, key:string, ...)
---@field sismember fun (self:resty.redis, key:string, ...)
---@field smembers fun (self:resty.redis, key:string, ...)
---@field sort fun (self:resty.redis, key:string, ...)
---@field srem fun (self:resty.redis, key:string, ...)
---@field sunion fun (self:resty.redis, key:string, ...)
---@field unsubscribe fun (self:resty.redis, key:string, ...) @  --/usr/local/openresty/lualib/resty/redis.lua:316
---@field zadd fun (self:resty.redis, key:string, ...)
---@field zincrby fun (self:resty.redis, key:string, ...)
---@field zrange fun (self:resty.redis, key:string, ...)
---@field zrangebyscore fun (self:resty.redis, key:string, ...)
---@field zrank fun (self:resty.redis, key:string, ...)
---@field zrem fun (self:resty.redis, key:string, ...)

