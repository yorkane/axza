local sfind, sub, char, ins, byte = string.find, string.sub, string.char, table.insert, string.byte
local upper, pcall, type, traceback = string.upper, pcall, type, debug.traceback
local nsub = ngx.re.gsub
local new_tab = require('table.new')
local floor = math.floor
local clone_tab = require("table.clone")
local ctxvar = require('resty.ctxvar')
local print, error, pairs, nreq = ngx.print, error, pairs, ngx.req
local json = require("cjson.safe")
local json_decode, json_encode = json.decode, json.encode
local utils = require('klib.utils')
local klass = require('klib.klass')
local dump = require('klib.dump')
local bit_full = utils.bit_full
local ok, template = pcall(require, "resty.template")
if not ok then
    template = nil
end

local function split(text, splitter, start_index, enable_empty_slots)
    if not text then
        return {}
    end
    start_index = start_index or 1
    splitter = splitter or ','
    local size = floor(#text / 7) + 3
    if size > 5 and size < 10 then
        size = 10
    end
    if size > 40 then
        size = 40
    end
    local arr = new_tab(size, 0)
    if #splitter == 1 then
        local len = #text
        local spb = byte(splitter)
        local last_match = start_index
        local nc, str = 1
        for i = start_index, len do
            if spb == byte(text, i, i) then
                if enable_empty_slots or (i - 1 - last_match) > -1 then
                    str = sub(text, last_match, i - 1)
                    arr[nc] = str
                    nc = nc + 1
                end
                last_match = i + 1
            end
        end
        if len >= last_match then
            arr[nc] = sub(text, last_match, len)
        end
        return arr
    end
    local temp = sub(text, start_index, #text)
    local len, width = 0, #splitter
    local nc = 1
    while true do
        len = sfind(temp, splitter, 1, true)
        if len ~= nil then
            local result = sub(temp, 1, len - 1)
            temp = sub(temp, len + width)
            --table.insert(res, result)
            arr[nc] = result
            nc = nc + 1
        else
            if #temp > 0 then
                arr[nc] = temp
            end
            break
        end
    end
    return arr
end

local escape_regex_re = [[([\.\\\[\]\$\^\*\?\+\{\}\(\)])]]
local function escape_regex(reg_str)
    return nsub(reg_str, escape_regex_re, [[\$1]], 'jo')
end

---@class klib.router
---@field root_entry string @ initial entry
---@field start_index number @ entry length
---@field router_map table<number, klib.router.map[]> @ entry length
---@field pre_access fun(params:table<string, string>, env:resty.ctxvar, req):boolean @ excute this function before request, if return true to proceed
---@field err_handler table<number, fun(env:resty.ctxvar, status:number, err:table, router:klib.router)> @ custom handler for mapping to status code: 4xx 5xx
---@field filter table<number, fun(output:string, ...)> @ apply filter as the push order  to the output content in router.json/router.print functions, and the following parameters required you pass it manually
local _M = {}
local _mt = { __index = _M }
setmetatable(_M, {
    __call = function(_M, root_entry)
        return _M.new(root_entry)
    end
})

local function correct_entry(str)
    if not str or str == '' then
        return '/'
    end
    if byte(str, 1, 1) ~= 47 then
        str = '/' .. str
        --error(str .. ' : rule_string must start with `/`' .. traceback('debug', 1))
    end
    return str
end

function _M.new(root_entry)
    root_entry = correct_entry(root_entry)
    local inst = {
        root_entry = root_entry,
        start_index = #root_entry,
        router_map = {},
        err_handler = {},
        filter = {},
        pre_access = nil,
        template = nil,
    }
    setmetatable(inst, _mt)
    return inst
end

local method_map = { GET = 1, HEAD = 2, POST = 4, PUT = 8, DELETE = 16, OPTIONS = 32, PATCH = 64 }

---parse_rule_map
---@param self klib.router
---@param rule_string string
---@param method string
---@return klib.router.map, string @ map and name of current map
local function parse_rule_map(self, rule_string, method)
    local bits = method_map[method]
    if not bits then
        return nil, 'bad method input, only GET/HEAD/POST/PUT/DELETE/PATCH/OPTIONS support'
    end
    local uid_str = ''
    local arr = split(rule_string, '/')
    local len, entry_name = #arr, arr[1] or '/'
    local map
    for i = 1, len do
        local param = arr[i]
        if sfind(param, ':', 1, 'true') == 1 then
            if not map then
                map = {}
            end
            local name = sub(param, 2, 200)
            map[i - 1] = name -- ignore first element
            uid_str = uid_str .. [[/[^/]+]]
            arr[i] = false
        else
            uid_str = uid_str .. '/' .. escape_regex(param)
        end
    end
    -- reduce level when only one params exist
    table.remove(arr, 1)
    if #arr == 1 then
        arr = arr[1] or nil
        if map then
            map = map[1]
        end
    elseif #arr == 0 then
        arr = nil
    end
    bits = bits + bit_full[len + 7] - bit_full[7]
    ---@class klib.router.map
    ---@field map string[] @preserve parameter/value sequence mapping
    local map_item = {
        uid = method .. '\t' .. self.root_entry .. uid_str,
        rule = rule_string,
        map = map,
        bits = bits,
        len = len,
        method = method,
        sequence = arr,
    }
    return map_item, entry_name
end

local function remove_query(path)
    if path then
        local inx = sfind(path, '?', 1, true) -- last uri section end with `?` will accept as query
        if inx then
            path = sub(path, 1, inx - 1)
        end
    end
    return path
end

function _M:build_response(defaul_kv, mt)
    local m = clone_tab(defaul_kv)
    m.root_entry = self.root_entry
    if mt and type(mt) == 'table' then
        setmetatable(m, mt)
    end
    return m
end

---get_request
---@param self klib.router
---@param uri string
---@param method string
---@param section_array string[] @ already split url sections
---@return klib.router.map, table<string, string> @ get map and params generated with rule_string
function _M:get_request(uri, method, section_array)
    local start_index = self.start_index
    local root_uri = sub(uri, 1, start_index)
    if root_uri ~= self.root_entry then
        return nil, nil, 'do not match to the root path, wrong entry for uri'
    end
    section_array = section_array or split(uri, '/', start_index + 1) --remove the prefix path besides root
    local section_len, entry_name = #section_array
    entry_name = section_array[1] or '/'
    if section_len == 1 and byte(section_array[1], 1, 1) == 63 then
        section_len = 0 -- treat /xxx?sss as /xxx
        entry_name = '/'
    end
    local bits = method_map[method] -- get method + slots info
    bits = bits + (bit_full[section_len + 7]) - bit_full[7] -- ignore the first 7 bits as preserved methods slot
    local arr, params, router_map = self.router_map[bits] -- get sequence arr from router_map by bits
    if arr then
        arr = arr[entry_name] or arr[remove_query(entry_name)]
    end
    --logs(arr, section_array, entry_name, bits)
    if arr then
        params = {}
        if section_len < 2 then
            router_map = arr --only one match in root
        elseif section_len == 2 then
            for i = 1, #arr do
                local sequence = arr[i].sequence
                local seq = remove_query(section_array[2])
                if sequence and sequence == seq then
                    router_map = arr[i]
                    break
                elseif not sequence and arr[i].map then
                    router_map = arr[i]
                    params[arr[i].map] = seq
                    break
                end
            end
        else
            for i = 1, #arr do
                local map = arr[i]
                local sequence = map.sequence
                local sequence_len = #sequence
                local is_match = true
                -- ignore first section compare
                for n = 1, sequence_len do
                    local chars = sequence[n]
                    local seq = section_array[n + 1]
                    -- by comparing every section in slots
                    if chars and chars ~= seq then
                        if sequence_len == n then
                            is_match = (remove_query(seq) == chars) -- Test last seq of the uri
                        else
                            is_match = false
                            break
                        end
                    end
                end
                if is_match then
                    -- the first complete match will be return
                    router_map = map
                    break
                end
            end
            if router_map then
                if router_map.map then
                    for i, v in pairs(router_map.map) do
                        params[v] = section_array[i + 1] -- Cause the first entry path as itself[A] will be ignored /A/B/C
                    end
                end
            end
        end
    end
    return router_map, params
end

---register map function to router
---@param rule_string string
---@param func fun(params:table<string, string>, env:resty.ctxvar, req:klib.router.request):number
---@param method string
---@param template string @ Nullable, template for compile output result
---@return fun, klib.router.map, string @ function injected, router.map hit the uri, error message
function _M:register(rule_string, func, method, template)
    rule_string = correct_entry(rule_string)
    local map, entry_name = parse_rule_map(self, rule_string, method)
    if not map then
        return nil, nil, entry_name
    end
    if not map then
        return nil, nil, 'Bad format, at least one param name required in :' .. rule_string
    end
    local router_map = self.router_map
    --prevent
    if router_map[map.uid] ~= nil then
        return nil, nil, 'duplicate func register in get:' .. rule_string
    end
    map.func = func
    map.template = template
    router_map[map.uid] = map
    local dic = router_map[map.bits]
    if not dic then
        dic = {}
        router_map[map.bits] = dic
    end
    if map.len < 2 then
        dic[entry_name] = map -- reduce cascade levels
    else
        local arr = dic[entry_name]
        if not arr then
            arr = { map }
            dic[entry_name] = arr
        else
            ins(arr, map)
        end
        if #arr > 1 then
            --put the /:xxx/:xxx mapped path/params type route to the tail of the array, avoid bad match with regular paths
            table.sort(arr, function(a, b)
                local c = a.map and 1 or 0
                local d = b.map and 1 or 0
                return d > c
            end)
        end

    end
    map.bits = nil --remove bits for saving memory
    return func, map
end

---html append content to nginx output buffer
---@param text string
---@param env resty.ctxvar @Nullable
---@param router_map klib.router.map @Nullable for applying router.filter
function _M:html(text, env, router_map)
    ngx.header['Content-Type'] = 'text/html; charset=UTF-8'
    if router_map then
        local filter = self.filter[router_map.uid] or self.filter
        for i = 1, #filter do
            text = filter[i](text, env, self)
        end
    end
    print(text)
end

---json append content to nginx output buffer
---@param luaobj table
---@param env resty.ctxvar @Nullable
---@param router_map klib.router.map @Nullable for applying router.filter
function _M:json(luaobj, env, router_map)
    local text
    if router_map.template and template then
        ngx.header['Content-Type'] = 'text/html; charset=UTF-8'
        text = template.compile(router_map.template)(luaobj)
    else
        ngx.header['Content-Type'] = 'application/javascript; charset=UTF-8'
        text = json_encode(luaobj)
    end
    if router_map then
        local filter = self.filter[router_map.uid] or self.filter
        for i = 1, #filter do
            text = filter[i](text, env, self)
        end
    end
    print(text)
end

---default_error_handler
---@param env resty.ctxvar
---@param status number
---@param err table
---@param self klib.router
local function default_error_handler(env, status, err, params, self)
    if env.is_json then
        ngx.header['Content-Type'] = 'application/json; charset=UTF-8'
        if not err.err then
            err = { err = err }
        end
        err.params = params
        print(json_encode(err))
    else
        if type(err) == table then
            err = json_encode(err)
        end
        ngx.header['Content-Type'] = 'text/html; charset=UTF-8'
        print('<h2>Internal error : ', status, '</h4><hr /><pre>error messages:<br />', err
        , '<hr />params:', json_encode(params)
        , '<hr/>Headers:', json_encode(ngx.req.get_headers()))
        --dump(self)
    end
end

---@class klib.router.request
local req = {}

function req.get_query()
    return ngx.req.get_uri_args()
end

---get_body_header
---@param env resty.ctxvar
---@return table<string, string>, table<string, string> @ body and header
function req.get_body_header(env)
    env = ctxvar(env)
    ngx.req.read_body()
    local headers = env.request_header
    local body = ngx.req.get_body_data()
    local a, b = byte(body, 1, 2)
    if a == 123 or a == 91 then
        if b == 123 or b == 91 or b == 34 then
            body = json.decode(body)
            return body, headers
        end
    end
    local header = headers['Content-Type']
    -- the post request have Content-Type header set
    if header then
        if sfind(header, "application/x-www-form-urlencoded", 1, true) then
            local post_args = ngx.req.get_post_args()
            if post_args and type(post_args) == "table" then
                body = post_args
            end
        elseif sfind(header, "json", 1, true) then
            body = json_decode(body)
            -- form-data request
            --elseif sfind(header, "multipart", 1, true) then
            -- upload request, should not invoke ngx.req.read_body()
            -- parsed as raw by default
        else
            -- body = ngx.req.get_body_data()
            -- local b = byte(body, 1, 1)
            -- if b == 123 or b == 91 then
            -- 	body = json.decode(body)
            -- end
        end
    else

    end
    return body, header
end

---handle usually put this nginx.conf location content_by_lua_block entry
---@param env resty.ctxvar @Nullable
function _M:handle(env)
    env = ctxvar(env)
    local map, params, err, status, result, ok, func, access_stop = self:get_request(env.request_uri, env.method)
    if map then
        if self.pre_access then
            access_stop, err = self.pre_access(func, params, env, req)
        end
        if not access_stop then
            func = map.func
            ok, result, status = xpcall(func, traceback, params, env, req)
            if not ok then
                err = result
                status = 500
            else
                if status and type(status) == 'number' then
                    ngx.status = status
                end
                if result and result ~= '' then
                    if type(result) == 'table' then
                        self:json(result, env, map)
                    else
                        self:html(result, env, map)
                    end
                end
                return
            end
        end
    else
        err = 'No router match to : ' .. env.request_uri
        status = 404
    end
    -- error handling
    ngx.status = status
    local err_handler = self.err_handler[status] or default_error_handler
    err_handler(env, status, err, params, self)
end

---error_handle
---@param status number
---@param func fun(env:resty.ctxvar, status:number, err:table, router:klib.router)
function _M:error_handle(status, func)
    local error_handler = self.err_handler
    if error_handler[status] then
        return nil, status .. ' error handler already registered!'
    end
    error_handler[status] = func
end

---merge merge other router with current router as the root entry
---@param router klib.router
---@param entry_string string @ the url entry followed with upper root
function _M:merge(router, entry_string)
    if entry_string then
        entry_string = correct_entry(entry_string)
    else
        entry_string = router.root_entry
    end
    for i, v in pairs(router.router_map) do
        if type(i) == 'string' then
            local func, map, err = self:register(entry_string .. v.rule, v.func, v.method)
            if #router.filter > 0 then
                self.filter[map.uid] = router.filter
            end
        end
    end
end

---add_filter append output content filter
---@param func fun(output:string, env:resty.ctxvar, current_router:klib.router):string @function to process content before output to nginx buffer
function _M:add_filter(func)
    ins(self.filter, func)
end

function _M:add_access(func)
    ins(self.access, func)
end

local methods = { GET = 'GET', HEAD = 'HEAD', POST = 'POST', PUT = 'PUT', DELETE = 'DELETE', OPTIONS = 'OPTIONS', PATCH = 'PATCH',
                  get = 'GET', head = 'HEAD', post = 'POST', put = 'PUT', delete = 'DELETE', options = 'OPTIONS', patch = 'PATCH',
                  [2] = 'GET', [4] = 'HEAD', [8] = 'POST', [16] = 'PUT', [32] = 'DELETE', [512] = 'OPTIONS', [16384] = 'PATCH'
}

---attach_module automatically register module method with router, please notice the `params` injected will be an empty table
---@param module_obj table<string, fun(params:table<string, string>, env:resty.ctxvar, req:klib.router.request)> @lua module / component / table
---@return table<string, table> @ return the registered method and parameters
function _M:attach_module(module_obj)
    local result = {}
    for fname, func in pairs(module_obj) do
        if type(func) == 'function' then
            local arr = split(fname, '_')
            local method, subname = arr[1], arr[2]
            if methods[method] then
                local finfo = klass.parse_func(func)
                if #finfo.params == 3 and finfo.params[1] == 'params' and finfo.params[2] == 'env' and finfo.params[3] == 'req' then
                    result[subname] = finfo.params
                    self:register('/' .. subname, func, method)
                elseif #finfo.params == 4 and finfo.params[1] == 'self' and finfo.params[2] == 'params' and finfo.params[3] == 'env' and finfo.params[4] == 'req' then
                    result[subname] = finfo.params
                    self:register('/' .. subname, function(params, env, req)
                        return func(module_obj, params, env, req) -- inject with self
                    end, method)
                end
            end
        end
    end
    return result
end

---@param rule_string string
---@param func fun(params:table<string, string>, env:resty.ctxvar, req:klib.router.request):number
---@return fun, klib.router.map, string @ function injected, router.map hit the uri, error message
function _M:get(rule_string, func, template)
    return self:register(rule_string, func, 'GET', template)
end
---@param rule_string string
---@param func fun(params:table<string, string>, env:resty.ctxvar, req:klib.router.request):number
---@return fun, klib.router.map, string @ function injected, router.map hit the uri, error message
function _M:post(rule_string, func, template)
    return self:register(rule_string, func, 'POST', template)
end
---@param rule_string string
---@param func fun(params:table<string, string>, env:resty.ctxvar, req:klib.router.request):number
---@return fun, klib.router.map, string @ function injected, router.map hit the uri, error message
function _M:delete(rule_string, func, template)
    return self:register(rule_string, func, 'DELETE', template)
end
---@param rule_string string
---@param func fun(params:table<string, string>, env:resty.ctxvar, req:klib.router.request):number
---@return fun, klib.router.map, string @ function injected, router.map hit the uri, error message
function _M:options(rule_string, func, template)
    return self:register(rule_string, func, 'OPTIONS', template)
end
---@param rule_string string
---@param func fun(params:table<string, string>, env:resty.ctxvar, req:klib.router.request):number
---@return fun, klib.router.map, string @ function injected, router.map hit the uri, error message
function _M:put(rule_string, func, template)
    return self:register(rule_string, func, 'PUT', template)
end
---@param rule_string string
---@param func fun(params:table<string, string>, env:resty.ctxvar, req:klib.router.request):number
---@return fun, klib.router.map, string @ function injected, router.map hit the uri, error message
function _M:head(rule_string, func, template)
    return self:register(rule_string, func, 'HEAD', template)
end
---@param rule_string string
---@param func fun(params:table<string, string>, env:resty.ctxvar, req:klib.router.request):number
---@return fun, klib.router.map, string @ function injected, router.map hit the uri, error message
function _M:patch(rule_string, func, template)
    return self:register(rule_string, func, 'PATCH', template)
end


--GET = ngx.HTTP_GET, -- 2
--HEAD = ngx.HTTP_HEAD, -- 4
--POST = ngx.HTTP_POST, -- 8
--PUT = ngx.HTTP_PUT, --16
--DELETE = ngx.HTTP_DELETE, -- 32
--MKCOL = ngx.HTTP_MKCOL, -- 64
--COPY = ngx.HTTP_COPY, -- 128
--MOVE = ngx.HTTP_MOVE, -- 256
--OPTIONS = ngx.HTTP_OPTIONS, --512
--PATCH = ngx.HTTP_PATCH, --16384
function _M.main()
    require('klib.dump').global()
    local r = _M.new('/test')
    r:get('/m1', function(param, env)
        dump(param, 'm1hit')
    end)
    r:post('/k1/:seek/:form', function(param, env)
        dump(param, 'seek1')
    end)

    r:post('/k1/:seek', function(param, env)
        dump(param, 'seek2')
    end)
    r:post('/xx1', function(param, env)
        dump(param, 'xx1')
    end)
    local uri = '/test/k1/xx1/'
    ngx.say(uri)
    r:handle({ request_uri = uri, method = 'POST' })
end
return _M

--GET = ngx.HTTP_GET, -- 2
--HEAD = ngx.HTTP_HEAD, -- 4
--POST = ngx.HTTP_POST, -- 8
--PUT = ngx.HTTP_PUT, --16
--DELETE = ngx.HTTP_DELETE, -- 32
--MKCOL = ngx.HTTP_MKCOL, -- 64
--COPY = ngx.HTTP_COPY, -- 128
--MOVE = ngx.HTTP_MOVE, -- 256
--OPTIONS = ngx.HTTP_OPTIONS, --512
--PATCH = ngx.HTTP_PATCH, --16384