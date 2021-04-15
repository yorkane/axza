require('resty.core')
require('klib.common')
local json = require("lib.json")
local aes = require "resty.aes"
local ins, tonumber, fmod, tostring = table.insert, tonumber, math.fmod, tostring
local nvar, concat, array, nctx, nreq, nphase, hash = ngx.var, table.concat, table.array, ngx.ctx, ngx.req, ngx.get_phase, table.hash
local now, update_time, time = ngx.now, ngx.update_time, ngx.time
local nfind, nsub = ngx.re.find, ngx.re.gsub
local nmatch, gmatch, byte, char = ngx.re.match, ngx.re.gmatch, string.byte, string.char
local find, sub, lower, rep, is_empty, split, is_domain, root_domain = string.find, string.sub, string.lower, string.rep, string.is_empty, string.split, string.is_domain, string.root_domain
local random, randomseed, floor = math.random, math.randomseed, math.floor
local ctxenv = require('lib.ctxenv')
local wid = ngx.worker.id() or 0
local current_dir = ngx.config.prefix()
if #current_dir < 5 or find(lower(current_dir), 'te*mp') then
	local lfs = require('lfs_ffi')
	if lfs and lfs.currentdir then
		current_dir = nsub(lfs:currentdir() .. '/', [[\\+]], '/', 'jo')
	end
	current_dir = nsub(current_dir, [[/nginx/.+]], '/nginx/', 'jo')
end
local inx = find(current_dir, '/lua/test/', 5, true)
if inx then
	current_dir = sub(current_dir, 1, inx)
end

local root = current_dir
local _M = {
	current_dir = current_dir,
	pid = wid .. '/' .. ngx.worker.count() or '0' .. '#' .. wid,
	root = root
}

local get_env = ctxenv.get_env

---build_url
---@param env system.env
---@param safe_host string @for overwrite local ip or none domain request
function _M.build_url(env, safe_host)
	env = ctxenv(env)
	local host = env.host
	if not is_domain(host) and safe_host then
		host = safe_host
	end
	local scheme, port, port_str = env.scheme .. '://', env.port

	if port == '443' and scheme == 'https://' then
		port_str = ''
	elseif port == '80' and scheme == 'http://' then
		port_str = ''
	else
		port_str = ':' .. port
	end
	return scheme .. host .. port_str .. env.request_uri
end

---get_full_url
---@param host string @ Nullable, customize a new full url with injected host
---@param ignore_port boolean @ Nullable true to ignore port eg: http://www.test.com:81/test.htm => http://www.test.com/test.htm
---@param is_root_url boolean @get full url or just root part eg: http://www.test.com/test.htm => http://www.test.com/
---@param ignore_protocl boolean @ https://test.test.com/test => test.test.com/test
---@return string @ full url
function _M.get_full_url(host, ignore_port, is_root_url, ignore_protocol)
	local env = get_env()
	local port, port_str = env.port
	host = host or env.host
	local scheme = ignore_protocol and '' or env.scheme .. '://'
	if ignore_port then
		port_str = ''
	else
		if port == '443' and scheme == 'https://' then
			port_str = ''
		elseif port == '80' and scheme == 'http://' then
			port_str = ''
		else
			port_str = ':' .. port
		end
	end
	return scheme .. host .. port_str .. (is_root_url and '' or env.request_uri)
end

---build_url_by relative url, and start with /
---@param url string @ must start with /
function _M.build_url_by(url, host)
	local env, port_str = get_env()
	host = host or env.host
	if (env.port == '443' and env.scheme == 'https://') or env.port == '80' then
		port_str = ''
	else
		port_str = ':' .. env.port
	end
	return env.scheme .. '://' .. host .. port_str .. url
end

---get_root_domain gate.test.com => test.com
---@param host string @ host to parse
---@return string, boolean @ is domain or not
function _M.get_root_domain(host)
	if not host then
		local env = get_env()
		host = env.host
	end
	local root = root_domain(host)
	if root then
		return root, true
	end
	return host, false
end

local get_env = _M.get_env
---parse_url
---@param url string
---@param default_host string @ overwrite request host as it's relative path like /xxx/xxx?xxxx
---@param is_with_parameters boolean @default false, parameter object cpu costs is high
---@return parsed_url
function _M.parse_url(url, default_host, is_with_parameters)
	if is_empty(url) then
		return nil
	end
	local scheme, host, port, uri, root, uobj, from1, to1, query, len, args = '://'
	local from, to = find(url, scheme, 1, true)
	if not from or from > 7 then
		local env = get_env()
		-- /foo?hello=1
		scheme = env.scheme
		port = env.port
		host = default_host or env.host
	else
		scheme = lower(sub(url, 1, from - 1))  -- sub https?
		url = sub(url, to + 1, #url) --sub the rest of url
		from, to = find(url, '/', 1, true)
		if not from then
			-- www.foo.com:81
			-- www.foo.com
			to = #url + 1 --sub the rest of url
		end
		host = sub(url, 1, to - 1)
		url = sub(url, to, #url)

		from, to = find(host, ':') -- deal the port part www.foo.com:81
		if from then
			port = tonumber(sub(host, from + 1, #host))
			host = sub(host, 1, from)
		end
		if not port then
			port = scheme == 'http' and 80 or 443
		end
	end
	len = #url
	local rest_url = url
	from, to = find(url, '?', 3, true)
	if from then
		query = sub(url, to + 1, len)
		rest_url = sub(url, 1, from - 1)
	end
	from, to = find(rest_url, '/', 2, true)  -- root = first [/] between second [/]
	if from then
		-- /abcdefg/query?v=123, root =/abcdefg
		root = sub(rest_url, 1, from)
	else
		root = rest_url
	end
	if is_with_parameters then
		args = _M.get_query_arg(query)
	end
	---@class parsed_url
	local uobj = {
		scheme = scheme,
		host = host,
		port = port,
		request_uri = url,
		root = root or '/',
		query = query,
		args = args
	}
	return uobj
end

---get_query_arg get single key's value from query
---@param query string @
---@param arg_key string @[Nullable] pickup value mapped to key in query
---@return string @single value mapped to the key
function _M.get_query_arg(query, arg_key)
	local from, from1, nc, flag = 0, 0, 0, 10
	if arg_key then
		local mk = arg_key .. '='
		local _, si = find(query, mk, 1, true)
		if si then
			si = si + 1
		else
			return -- fail to find key
		end
		local ei = find(query, '&', si, true)
		if ei then
			ei = ei - 1
		else
			return sub(query, si, -1) -- at the tail of the query string
		end
		return sub(query, si, ei)
	end
end

---get_query_args
---@param query string @
---@param result_tab table<string, string> @ to inject temp table to improve performance
---@return table<string, string>
function _M.get_query_args(query, result_tab)
	result_tab = result_tab or hash(7)
	local len = #query
	local lasti, last_key, value = 1
	for i = 1, len do
		local code = byte(query, i)
		if code == 63 then
			-- ?
			lasti = i + 1
		elseif code == 61 then
			last_key = sub(query, lasti, i - 1) -- =
			lasti = i + 1
		elseif code == 38 then
			value = sub(query, lasti, i - 1) -- &
			lasti = i + 1
			result_tab[last_key] = value
			last_key = nil
		end
	end
	if last_key then
		result_tab[last_key] = sub(query, lasti, len)
	end

	return result_tab
end

---remove_query_arg arg key and value from query string
---@param query string @query string
---@param arg string @argument name
function _M.remove_query_arg(query, arg, only_query_part)
	local start_index = 1
	local arg_len = #arg
	if byte(arg, arg_len) ~= 61 then
		arg = arg .. '='
	end
	local inx, end_inx
	if only_query_part and byte(query, 1) ~= 38 then
		query = '&' .. query
	else
		local arg2 = '?' .. arg
		inx = find(query, arg2, 1, true)
		if inx then
			end_inx = find(query, '&', inx + arg_len, true)
			if end_inx then
				query = sub(query, 1, inx, end_inx - 1) .. sub(query, end_inx + 1, -1) -- remove `?xxx=yyy&zzz=vvvv` = ?zzz=vvvv
			else
				query = sub(query, 1, inx - 1, -1) --remove `/path/?xxx=yyy` = /path/
			end
		end
	end
	arg = '&' .. arg
	for i = 1, 20 do
		inx = find(query, arg, start_index, true)
		if not inx then
			break
		end
		end_inx = find(query, '&', inx + arg_len, true)
		if end_inx then
			query = sub(query, 1, inx - 1, end_inx - 1) .. sub(query, end_inx, -1)
		else
			query = sub(query, 1, inx - 1, -1)
			break
		end
		start_index = end_inx
	end
	if byte(query, 1) == 38 then
		return sub(query, 2, -1)
	end
	return query
end

---pick_query_args pickup formatted target args from query string, for filtering args within the arg_list
---@param query string @ query to filter
---@param arg_list string[] @ argument key list for filtering
---@return string @ filter query string
function _M.pick_query_args(query, arg_list, only_query_part)
	local start_index, initial_inx
	local arg, result, inx, end_inx
	local nc = 0
	if not only_query_part then
		initial_inx = find(query, '?', 1, true)
		if initial_inx then
			result = sub(query, 1, initial_inx)
		else
			result = ''
			initial_inx = 1
		end
	else
		result = ''
		initial_inx = 1
	end

	for n = 1, #arg_list do
		arg = arg_list[n]
		local arg_len = #arg
		arg = arg .. '='
		if only_query_part then
			query = '&' .. query
		else
			inx = find(query, '?' .. arg, initial_inx, true)
			if inx then
				end_inx = find(query, '&', inx, true)
				result = result .. sub(query, inx + 1, end_inx - 1)
				initial_inx = end_inx
				nc = nc + 1
			end
		end
		arg = '&' .. arg
		start_index = initial_inx
		for i = 1, 20 do
			inx = find(query, arg, start_index, true)
			if not inx then
				break
			end
			end_inx = find(query, '&', inx + arg_len, true)
			if end_inx then
				if nc == 0 then
					result = result .. sub(query, inx + 1, end_inx - 1) -- at middle
				else
					result = result .. sub(query, inx, end_inx - 1) -- at tail
				end
			else
				result = result .. sub(query, inx, -1)
				break
			end
			nc = nc + 1
			start_index = end_inx - 1
		end
	end
	return result
end

---replace_query_args
---@param query_args string @[Required]
---@param key_val_map table<string, string> @[Required] {name = 'replaced_name', address = 'fixed' }
---@return string @replaced query string
function _M.replace_query_args(query_args, key_val_map)
	local inx = 1
	for key, val in pairs(key_val_map) do
		for i = 1, 20 do
			inx = find(query_args, key .. '=', inx, true)
			if inx then
				local is_found, is_head = false
				if inx > 1 then
					local chars = byte(query_args, inx - 1)
					if chars == 63 then
						is_found = true
						is_head = true
					elseif chars == 38 then
						is_found = true
					end
				end
				if is_found or inx == 1 then
					local len = #key
					local is_remove
					if val == '' then
						is_remove = true
					end
					local minus = 2
					local end_inx = find(query_args, '&', inx + len, true)
					if is_head and is_remove then
						minus = 1
						end_inx = end_inx + 1
					end
					if end_inx then
						if is_remove then
							query_args = sub(query_args, 1, inx - minus) .. sub(query_args, end_inx, -1)
						else
							query_args = sub(query_args, 1, inx + len) .. val .. sub(query_args, end_inx, -1)
						end
					else
						if is_remove then
							query_args = sub(query_args, 1, inx - minus)
						else
							query_args = sub(query_args, 1, inx + len) .. val
						end
					end
				end
				inx = inx + #key + 1
			else
				break
			end
		end
		inx = 1
	end
	return query_args
end

local _random_seed_nc = 1
function _M.random(startInt, endInt)
	local seed = floor((time() - ngx.req.start_time() + _random_seed_nc) * 10000) + wid
	randomseed(seed)
	_random_seed_nc = _random_seed_nc + 1
	return random(startInt, endInt)
end

---random_str
---@param count number @random chars count
---@return string @ random_text
function _M.random_text(count, seed, is_word, is_binary)
	seed = seed or 1
	local seed1 = ((now() - 1505731316) * 1000) + wid + (count * 1000)
	randomseed(seed + seed1)
	local tb = array(count)
	for i = 1, count do
		if is_binary then
			tb[i] = char(random(0, 255))
		elseif is_word then
			local nc = 0
			local cr = random(65, 122)
			while cr > 90 and cr < 97 do
				nc = nc + 1
				randomseed(cr + nc)
				cr = random(65, 122)
			end
			tb[i] = char(cr)
		else
			tb[i] = char(random(33, 126))
		end
	end
	return concat(tb, '')
end

---get_file_extension get the max 5 characters file extension
---@param url string
---@return string
function _M.get_file_extension(url)
	local inx = find(url, '?', 1, true)
	if inx then
		url = sub(url, 1, inx - 1)
	end
	local len = #url
	if len < 2 then
		return nil
	end
	if len < 7 then
		len = 7
	end
	local c1, c2, c3, c4, c5, c6 = byte(url, len - 6, len)
	--logs(url, byte(url, len - 6, len))
	-- char(46) = `.`
	if c1 == 46 then
		return sub(url, len - 5, len)
	end
	if c2 == 46 then
		return sub(url, len - 4, len)
	end
	if c3 == 46 then
		return sub(url, len - 3, len)
	end
	if c4 == 46 then
		return sub(url, len - 2, len)
	end
	if c5 == 46 then
		return sub(url, len - 1, len)
	end
	if c6 == 46 then
		return sub(url, len, len)
	end
end
local get_file_extension = _M.get_file_extension

local static_file_extension = '.js.css.html.htm.wpd.jpg.wbp.gif.png.bmp.ico.icon.webp.svg.rar.tar.gz.zip.ttf.eot.woff.swf.flv'
---is_static_url
---@param uri string
---@param env system.env
function _M.is_static_url(uri, env)
	if not uri and not env then
		env = get_env()
		if env.is_static then
			return true
		end
		uri = env.request_uri
	end
	local file = get_file_extension(uri)
	if file and find(static_file_extension, file, 1, true) then
		if env then
			env.is_static = true
		end
		return true
	end
end

---benchmark
---@param func fun(number_range:number)
---@param duration number @ the minimum execution milliseconds duration, the bigger the more accurate, but cost more time to complete benchmark
---@param range number @ int number ranges
---@return benchmark_result
---Demo: dump(_M.benchmark(string.find, str, 'insert', 1, true), 'sfind')
function _M.benchmark(func, count, duration, range)
	count = count or 20000
	local nc, log, next_count, res = 0, {}, count
	update_time()
	local start_time = now()
	local tms = 1
	local t1 = now()
	if range then
		local rnc = 1
		for i = 1, count do
			func(rnc)
			rnc = rnc + 1
			if rnc > range then
				rnc = 1
			end
		end
	else
		for i = 1, count do
			func(i)
		end
	end
	local tnc = 1
	update_time()
	tms = floor((now() - t1) * 1000 + 0.5)
	local first_run_mark = count / tms
	log[1] = count .. ' times in ' .. tms .. ' ms'
	if not duration or duration < 200 then
		duration = 200
	end
	while (tms < duration and nc < 10) do
		next_count = floor(next_count * (duration / tms) + 0.5)
		if tostring(next_count) == 'inf' then
			count = count * 20
			next_count = count
		else
			count = next_count
		end
		update_time()
		t1 = now()
		if range then
			local rnc = 1
			for i = 1, count do
				func(rnc)
				rnc = rnc + 1
				if rnc > range then
					rnc = 1
				end
			end
		else
			for i = 1, count do
				func(tnc)
				tnc = tnc + 1
			end
		end
		update_time()
		tms = floor((now() - t1) * 1000 + 0.5)
		ins(log, count .. ' times in ' .. tms .. ' ms')
		--ngx.say(tms, '|', count)
		nc = nc + 1
	end
	res = floor(count / tms)
	update_time()
	local info = debug.getinfo(func)
	local _info
	local defined
	if info then
		local file_path = sub(info.source, 2, #info.source)
		defined = ' --' .. file_path .. ' @' .. info.linedefined + 2
		if #file_path > 20 then
			local cnt = _M.read_file(file_path)
			if cnt then
				local arr = string.re_split(cnt, [[[\r]?\n]])
				local txt = ''
				for i = info.linedefined + 1, info.lastlinedefined + 2 do
					if find(arr[i], '--', 1, true) then
					else
						txt = txt .. arr[i]
					end
				end
				local mc = nmatch(txt, [[\w+[\.\:]?\w*\([^\)]+\)]])
				if mc then
					_info = mc[0]
				end
			end
		end
		info.source = nil
		info.linedefined = nil
		info.currentline = nil
		info.isvararg = nil
		info.lastlinedefined = nil
		info.namewhat = nil
		info.short_src = nil
		info.nups = nil
		info.nparams = nil
	end
	defined = defined or ''
	defined = defined .. string.dump(func, true)
	---@class benchmark_result
	return {
		first_run_mark = first_run_mark,
		mark = res,
		total_duration = ((now() - start_time) * 1000) .. ' ms',
		minimum_duration = duration,
		result = func(1),
		msg = 'Benchmarks run ' .. count .. ' times and completed in ' .. tms .. ' ms (1/1000 sec) average :' .. res .. ' /ms',
		log = log,
		first_line = (_info or '') .. (defined or ''),
	}
end

---benchmark_text text
---@param func fun(random_string:string, count_index:number) @function to be tested, could accept 2 parameters
---@param total_run_counts number @total execution counts
---@param random_text_length number @the length of random_text
---@param is_pure_word boolean @ the random_text only composed by alphabets
function _M.benchmark_text(func, total_run_counts, random_text_length, is_pure_word)
	local arr, res = table.array(total_run_counts)
	random_text_length = random_text_length or 30
	update_time()
	local t1, t2 = now()
	for i = 1, total_run_counts do
		arr[i] = _M.random_text(random_text_length, i, is_pure_word)
	end
	update_time()
	local prepare_time = math.floor((now() - t1) * 1000 + 0.5)
	update_time()
	t1 = now()
	for i = 1, total_run_counts do
		res = func(arr[i], i)
	end
	update_time()
	t2 = math.floor((now() - t1) * 1000 + 0.5)
	arr = nil
	collectgarbage('collect')
	return {
		mark = math.floor((total_run_counts / t2) + 0.5),
		run_duration = t2 .. ' ms',
		last_result = res,
		prepare_time = prepare_time .. 'ms',
		msg = 'Benchmarks run ' .. total_run_counts .. ' times and completed in ' .. t2 .. ' ms (1/1000 sec)',
	}
end

local tick_store = {}
function _M.start_tick(name)
	update_time()
	tick_store[name or 'timer'] = now()
end
function _M.end_tick(name)
	update_time()
	return 1000 * (now() - tick_store[name or name or 'timer'])
end

--read all string from a file
function _M.read_file(filename, read_tail, is_binary)
	local file, err = io.open(filename, "r")
	if not file then
		return nil, err
	end
	local str, err
	if read_tail then
		local current = file:seek()
		local fileSize = file:seek("end")  --get file total size
		if fileSize > 200000 then
			file:seek('set', fileSize - 200000) --move cusor to last part
			str, err = file:read(200000)
		else
			file:seek('set', 0) --move cursor to head
			str, err = file:read(fileSize)
		end
	else
		str, err = file:read("*a")
	end
	file:close()
	str = str or ''
	return str, err
end

function _M.write_file(filename, str, is_overwrite, is_hashed, is_binary)
	local cont, err = _M.read_file(filename)
	if cont then
		if not is_overwrite then
			return 'File already exist!'
		end
		if str == cont then
			return 'File content are identical'
		end
		if is_hashed then
			if sub(cont, 1, 33) == sub(str, 1, 33) then
				return 'Same content hash'
			end
		end
	end
	local file, err = io.open(filename, is_binary and 'wb+' or 'w+')
	if not file then
		return err
	end
	local str, err = file:write(str)
	file:close()
	return err
end

---aes_encrypt
---@param plain_str string @plain text to entrypt
---@return string @entryted string with system secure key
function _M.aes_encrypt(plain_str, secure_key)
	if not plain_str then
		return nil, 'empty input for aes_encrypt'
	end
	if type(plain_str) ~= 'string' then
		plain_str = plain_str .. ''
	end
	local aes_128_cbc_md5 = aes:new(secure_key)
	-- the default cipher is AES 128 CBC with 1 round of MD5
	return aes_128_cbc_md5:encrypt(plain_str)
end
---aes_decrypt
---@param encrypted_str string @encrypted text by aes_encrypt method
---@return string @plain text descrypted
function _M.aes_decrypt(encrypted_str, secure_key)
	if not encrypted_str then
		return nil, 'empty input for aes_decrypt'
	end
	if type(encrypted_str) ~= 'string' then
		return nil, 'must be sting'
	end
	local aes_128_cbc_md5 = aes:new(secure_key)
	return aes_128_cbc_md5:decrypt(encrypted_str)
end

---flush_all_cache clear all the sharedDict cache
function _M.flush_all_cache()
	local tlc = require('resty.tlcache')
	tlc:flush_all()
	for i, v in pairs(ngx.shared) do
		v:flush_all()
	end
end

function _M.dofile(file)
	if find(file, '.', 2, true) then
		file = string.gsub(file, '%.', '/') .. '.lua'
	end
	local ok, res = pcall(dofile, _M.current_dir .. file)
	return res
end

---get_call_path
---@param stack_level string @ stack_level to ingore
---@param is_short_path boolean @ the result contains full path or just filename
function _M.get_call_path(stack_level, is_short_path)
	local nc, m, err, iterator = 4
	stack_level = stack_level or 2
	local msg = debug.traceback('debug', stack_level)
	if not msg then
		msg = debug.traceback('debug', stack_level - 1)
	end
	if is_short_path then
		iterator = gmatch(msg, [[(\w[\w]+\.+lua:\d+:) in function ('(\w+)'|<)]], 'jo')
	else
		iterator = gmatch(msg, [[(\w[\w\\\/\.]+\.+lua:\d+:) in function ('(\w+)'|<)]], 'jo')
	end
	local sb = string.buffer()
	while (nc > 0) do
		m, err = iterator()
		if m then
			if nc < 4 then
				sb:add(m[1], (m[3] and m[3] .. '()' or '()'), ' <= ')
			end
		else
			break
		end
		nc = nc - 1
	end
	return sb:pop():tos()
end

---get_localhost_ip get ip for localhost or virtual machine ip within container
---@return string
function _M.get_localhost_ip()
	local ip = os.exec([[hostname -i]]) -- for alpine
	ip = nmatch(ip, [[(\d+\.\d+\.\d+)\.]], 'jio')
	ngx.say(ip)
	if ip then
		return ip[1] .. '.1'
	end
	return '127.0.0.1'
end

---pack_string_args @ max 5 arguments accept for performance. more arguments using string list at first argument instead. The last argument could exceed 255 chars, other arguments must less than 255 char. Nil input will convert to empty string: ``
---@param arg1 string|string[] @ The first string argument, if it's string list, the following arguments will be ignored
---@param arg2 string @ within 255 chars
---@param arg3 string
---@param arg4 string
---@param arg5 string @ could exceed 255 chars if this is the last arguments
---@return string @serialized string
function _M.pack_string_args(arg1, arg2, arg3, arg4, arg5)
	if not arg1 then
		return
	end
	local data
	local tp = type(arg1)
	if tp == 'table' then
		local inx, key, val = 0
		while true do
			key, val = next(arg1, key)
			inx = inx + 1
			if key then
				if inx ~= key then
					arg1[inx] = '' -- fill `nil` array slot with `` empty string
				end
			else
				break
			end
		end
		local len = #arg1
		local pre = char(len)
		for i = 1, len - 1 do
			local arg = arg1[i]
			if not arg then
				arg1[i] = ''
				pre = pre .. char(0)
			else
				local n = #tostring(arg)
				if n > 255 then
					n = 255
					arg1[i] = sub(arg1[i], 1, 255)
				end
				pre = pre .. char(n)
			end
		end
		return pre .. concat(arg1)
	end
	if arg5 then
		arg5 = tostring(arg5)
		arg4 = tostring(arg4 or '')
		arg3 = tostring(arg3 or '')
		arg2 = tostring(arg2 or '')
		arg1 = tostring(arg1 or '')
		local n1, n2, n3, n4 = #arg1, #arg2, #arg3, #arg4
		if n1 > 255 then
			n1 = 255
			arg1 = sub(arg1, 1, 255)
		end
		if n2 > 255 then
			n2 = 255
			arg2 = sub(arg2, 1, 255)
		end
		if n3 > 255 then
			n3 = 255
			arg3 = sub(arg3, 1, 255)
		end
		if n4 > 255 then
			n4 = 255
			arg4 = sub(arg4, 1, 255)
		end
		data = char(5, n1, n2, n3, n4) .. arg1 .. arg2 .. arg3 .. arg4 .. arg5
	elseif arg4 then
		arg4 = tostring(arg4)
		arg3 = tostring(arg3 or '')
		arg2 = tostring(arg2 or '')
		arg1 = tostring(arg1 or '')
		local n1, n2, n3 = #arg1, #arg2, #arg3
		if n1 > 255 then
			n1 = 255
			arg1 = sub(arg1, 1, 255)
		end
		if n2 > 255 then
			n2 = 255
			arg2 = sub(arg2, 1, 255)
		end
		if n3 > 255 then
			n3 = 255
			arg3 = sub(arg3, 1, 255)
		end
		data = char(4, n1, n2, n3) .. arg1 .. arg2 .. arg3 .. arg4
	elseif arg3 then
		arg3 = tostring(arg3)
		arg2 = tostring(arg2 or '')
		arg1 = tostring(arg1 or '')
		local n1, n2 = #arg1, #arg2
		if n1 > 255 then
			n1 = 255
			arg1 = sub(arg1, 1, 255)
		end
		if n2 > 255 then
			n2 = 255
			arg2 = sub(arg2, 1, 255)
		end
		data = char(3, n1, n2) .. arg1 .. arg2 .. arg3
	elseif arg2 then
		arg2 = tostring(arg2)
		arg1 = tostring(arg1 or '')
		local n1 = #arg1
		if n1 > 255 then
			n1 = 255
			arg1 = sub(arg1, 1, 255)
		end
		data = char(2, n1) .. arg1 .. arg2
	elseif arg1 then
		arg1 = tostring(arg1)
		data = char(1) .. arg1
	end
	return data
end

---unpack_string_args correspond to pack_string_args, decode args from serialized string
---@param str string @ the serialized string arguments
---@param no_array boolean @Default with a string-list, set true to just return first 5 or less results
---@return string|string[], string, string, string, string
function _M.unpack_string_args(str, no_array)
	if not str then
		return nil
	end
	local len = #str
	local count = byte(str, 1)
	local last_inx = count + 1
	local end_inx = 0
	if no_array then
		if count > 5 then
			count = 5
		end
		if count == 5 then
			local n1, n2, n3, n4 = byte(str, 2, count)
			return sub(str, last_inx, last_inx + n1 - 1), sub(str, last_inx + n1, last_inx + n1 + n2 - 1), sub(str, last_inx + n1 + n2, last_inx + n1 + n2 + n3 - 1), sub(str, last_inx + n1 + n2 + n3, last_inx + n1 + n2 + n3 + n4 - 1), sub(str, last_inx + n1 + n2 + n3 + n4, len)
		elseif count == 4 then
			local n1, n2, n3, n4 = byte(str, 2, count)
			return sub(str, last_inx, last_inx + n1 - 1), sub(str, last_inx + n1, last_inx + n1 + n2 - 1), sub(str, last_inx + n1 + n2, last_inx + n1 + n2 + n3 - 1), sub(str, last_inx + n1 + n2 + n3, len)
		elseif count == 3 then
			local n1, n2, n3, n4 = byte(str, 2, count)
			return sub(str, last_inx, last_inx + n1 - 1), sub(str, last_inx + n1, last_inx + n1 + n2 - 1), sub(str, last_inx + n1 + n2, len)
		elseif count == 2 then
			local n1, n2, n3, n4 = byte(str, 2, count)
			return sub(str, last_inx, last_inx + n1 - 1), sub(str, last_inx + n1, len)
		elseif count == 1 then
			return sub(str, 2, len)
		end
	end
	local arr = array(count)
	for i = 1, count do
		arr[i] = byte(str, i + 1)
	end
	for i = 1, count - 1 do
		local arg_len = arr[i]
		end_inx = last_inx + arg_len
		arr[i] = sub(str, last_inx, end_inx - 1)
		last_inx = end_inx
	end
	arr[count] = sub(str, last_inx, len)
	return arr
end

local format_regs = [[\{\{(\w+).?(\w+)?\}\}]]
---format {{key}} or {{subfield.key}} with in template
---@param str_template string @[Required] template for formatting.
---@param tb_key_map table<string,string> @[Required] key string map inject for replacement
---@param reg_match string @[Default: \{\{(\w+).?(\w+)?\}\}]
function _M.format(str_template, tb_key_map, reg_match)
	reg_match = reg_match or format_regs
	local format_func = function(m)
		if m[2] then
			return tostring(tb_key_map[m[1]][m[2]])
		end
		return tostring(tb_key_map[m[1]])
	end
	local res = nsub(str_template, reg_match, format_func, 'jo')
	return res
end

--{{{
_M.bit_full = {
	[1] = 1,
	[2] = 3,
	[3] = 7,
	[4] = 15,
	[5] = 31,
	[6] = 63,
	[7] = 127,
	[8] = 255,
	[9] = 511,
	[10] = 1023,
	[11] = 2047,
	[12] = 4095,
	[13] = 8191,
	[14] = 16383,
	[15] = 32767,
	[16] = 65535,
	[17] = 131071,
	[18] = 262143,
	[19] = 524287,
	[20] = 1048575,
	[21] = 2097151,
	[22] = 4194303,
	[23] = 8388607,
	[24] = 16777215,
	[25] = 33554431,
	[26] = 67108863,
	[27] = 134217727,
	[28] = 268435455,
	[29] = 536870911,
	[30] = 1073741823,
	[31] = 2147483647,
	[32] = 4294967295,
	[33] = 8589934591,
	[34] = 17179869183,
	[35] = 34359738367,
	[36] = 68719476735,
	[37] = 137438953471,
	[38] = 274877906943,
	[39] = 549755813887,
	[40] = 1099511627775,
	[41] = 2199023255551,
	[42] = 4398046511103,
	[43] = 8796093022207,
	[44] = 17592186044415,
	[45] = 35184372088831,
	[46] = 70368744177663
}

_M.bit_slot = {
	[1] = 1,
	[2] = 2,
	[3] = 4,
	[4] = 8,
	[5] = 16,
	[6] = 32,
	[7] = 64,
	[8] = 128,
	[9] = 256,
	[10] = 512,
	[11] = 1024,
	[12] = 2048,
	[13] = 4096,
	[14] = 8192,
	[15] = 16384,
	[16] = 32768,
	[17] = 65536,
	[18] = 131072,
	[19] = 262144,
	[20] = 524288,
	[21] = 1048576,
	[22] = 2097152,
	[23] = 4194304,
	[24] = 8388608,
	[25] = 16777216,
	[26] = 33554432,
	[27] = 67108864,
	[28] = 134217728,
	[29] = 268435456,
	[30] = 536870912,
	[31] = 1073741824,
	[32] = 2147483648,
	[33] = 4294967296,
	[34] = 8589934592,
	[35] = 17179869184,
	[36] = 34359738368,
	[37] = 68719476736,
	[38] = 137438953472,
	[39] = 274877906944,
	[40] = 549755813888,
	[41] = 1099511627776,
	[42] = 2199023255552,
	[43] = 4398046511104,
	[44] = 8796093022208,
	[45] = 17592186044416,
	[46] = 35184372088832
}
--}}}
return _M