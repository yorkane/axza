local say, nexec, nvar, print, floor = ngx.say, ngx.exec, ngx.var, ngx.print, math.floor
local nmatch, gmatch, byte, char, sfind, is_empty, ssub = ngx.re.match, ngx.re.gmatch, string.byte, string.char, string.find, string.is_empty, string.sub
local insert, concat, format = table.insert, table.concat, string.format
local re_split = require('ngx.re').split
local count_hit = table.new(0, 10)
local json = require('cjson')
local tabpool = require('tablepool')
local TAG = '[MOCK]'
local dict = ngx.shared['cache']
if not dict then
	local key = next(ngx.shared)
	dict = ngx.shared[key]
end
local pid = ngx.worker.id() or 0 .. '/' .. ngx.worker.count() .. '#' .. ngx.worker.pid()

local _M = {
}

local _random_seed_nc = 1
function _M.random(startInt, endInt)
	local seed = math.floor((ngx.time() - ngx.req.start_time() + _random_seed_nc) * 10000) + ngx.worker.pid()
	math.randomseed(seed)
	_random_seed_nc = _random_seed_nc + 1
	return math.random(startInt, endInt)
end

---counter usually for request validation or performance test
---@param key string
function _M.counter(key)
	key = key or 'default' --ngx.var.arg_name
	local nc = count_hit[key] or 0
	nc = nc + 1
	count_hit[key] = nc
	--count_hit[key] = nc
	--ngx.header['x-counter'] = count_hit[key]
	--ngx.header.pid = pid
	local tnc = dict:incr('mock_counter', 1, 0)
	local str = nc .. '/' .. tnc .. '@' .. pid
	local ok = pcall(say, str)
	if ok then
	end
	return str
end

function _M.get_request_args()
	local args = ngx.req.get_uri_args()
	ngx.req.read_body()
	local post_args, err = ngx.req.get_post_args()
	if post_args == nil then
		return args
	end
	for k, v in pairs(post_args) do
		args[k] = v
	end
	return args
end

function _M.mock()
	local host = ngx.var.host
	local args = _M.get_request_args()
	local mock_hit = count_hit[host]
	if not mock_hit then
		mock_hit = 1
	else
		mock_hit = mock_hit + 1
	end
	local nc = dict:incr('mock_counter', 1, 0)
	count_hit[host] = mock_hit
	local body = args['body']
	if body then
		body = ngx.unescape_uri(body)
	end
	local sleep = tonumber(args['sleep']) or 0
	local random_sleep = tonumber(args['random']) or 0
	local status = args['status'] or 200
	local redirect = args['redirect']
	if redirect then
		redirect = ngx.unescape_uri(redirect)
		ngx.redirect(redirect)
		return
	end
	local size = tonumber(args['size']) or 1 --defa ult 1kb+ body size
	if random_sleep > 0 then
		if sleep > random_sleep then
			sleep = _M.random(random_sleep, sleep)
		else
			sleep = _M.random(sleep, random_sleep)
		end
	end
	if sleep > 0 then
		ngx.sleep(sleep * 0.001)
	end

	if status ~= 200 and #status > 6 then
		if sfind(status, '200', 1, true) then
			local url = ngx.var.request_uri
			if not dict:get(url) then
				status = 200
				dict:set(url, true) -- first time always 200
			end
		end
		if status ~= 200 then
			local status_arr = re_split(status, [[\|]], 'jo')
			local rd = _M.random(1, #status_arr)
			status = tonumber(status_arr[rd])
			if not status or status > 530 then
				status = 500
			end
		end
	end
	local tp = args['type'] or 'html'
	if tp == 'js' then
		tp = 'application/json; charset=UTF-8'
	end
	if tp == 'html' then
		tp = 'text/html; charset=UTF-8'
	end
	if tp == 'text' then
		tp = 'text/plain; charset=UTF-8'
	end
	ngx.header['Content-Type'] = tp
	ngx.status = status
	ngx.header['pid'] = pid
	ngx.header['x'] = ngx.now() .. 'sleep' .. (sleep or 0)
	if body then
	else
		local sb = {ngx.var.server_addr..':'..ngx.var.server_port..':'..ngx.var.server_name ..'-' ..host..ngx.var.request_uri, ' | pid: ' .. pid, '\n<pre>Hit count:' .. mock_hit .. '/' .. nc, '<br/>\n' .. ngx.localtime() .. '<hr >\n', ngx.req.raw_header(),'<hr />\nDemos: ',
			'?status=200|200|404|500&sleep=1&body={BODY_IS_HERE} --sleep 1 sec and random status code in rates 200|200|404|500',
			'?sleep=10&random=5000&body={BODY_IS_HERE}&502  --random sleep 10-5000 millisecond with 502kb body ',
			'?sleep=3000&random=500&body={BODY_IS_HERE}&size=5  --random sleep 500-3000 millisecond with 5kb + body<hr />',
			'body-size = ' .. (size) .. 'kb<hr/>\nResponse Headers:'
		}
		local header = ngx.resp.get_headers()
		for i, v in pairs(header) do
			insert(sb, i..': '..v)
		end
		insert(sb, '<hr/>\nRequest Arguments:')
		for i, v in pairs(args) do
			if type(v) == 'table' then
				insert(sb, i..': '..json.encode(v))
			else
				insert(sb, i..': '..tostring(v))
			end
		end
		body = concat(sb, "\n")
		if size > 1 then
			body = body .. '\n</pre><!--' .. string.rep('1234567890', size * 102) .. '-->'
		end
		--say(ngx.req.raw_header())
		--say('<!--', string.rep('1234567890', size * 102), '-->')
	end
	ngx.header['Content-Length'] = #body
	print(body)
	--ngx.say('\n\n', ngx.var.request_uri)

end

function _M.getNgxInfo()
	local tb = {}
	insert(tb, format("url: %s", ngx.var.request_uri))
	insert(tb, format("Request header: %s", json.encode(ngx.req.get_headers())))
	insert(tb, format("Response header: %s", json.encode(ngx.resp.get_headers())))
	insert(tb, format(pid))
	return concat(tb, '\n')
end

return _M
