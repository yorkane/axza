local json = require("lib.json")
local utils = require("klib.utils")
local gsub, get_env, pairs, is_domain, is_static_url, concat, ins = ngx.re.gsub, utils.get_env, pairs, string.is_domain, utils.is_static_url, table.concat, table.insert

local say, nexec, nvar, print, floor = ngx.say, ngx.exec, ngx.var, ngx.print, math.floor
local nmatch, gmatch, byte, char, sfind, is_empty, ssub, split, nfind, nsub = ngx.re.match, ngx.re.gmatch, string.byte, string.char, string.find, string.is_empty, string.sub, string.split, ngx.re.find, ngx.re.gsub

local ngx_re = require "ngx.re"
local file = require('klib.file')
local template = require("resty.template")
local root = '/usr/local/openresty/nginx'
local dump, logs, dump_class, dump_lua, dump_doc, dump_dict = require('klib.dump').locally()
template.result = function(view, context, key, plain)
	return template.compile(view, key, plain)(context)
end

local _M = {
}

---get_lua_docs
---@param folder_list string|string[] @folder path or folder path list
---@return table<string, lua_doc>, table<string, lua_doc> @ extended classes, base classes
function _M.get_lua_docs(folder_list)
	local root = root .. '/lua/'
	---@type lua_doc
	local doc = {}
	---@type lua_doc
	local class_doc = {}
	local flist = _M.search_file(folder_list, nil, [[\.lua$]])
	for i = 1, #flist do
		local file_name = flist[i]
		local file = utils.read_file(file_name)
		local funs = {}
		local reg = [[---([a-z]+[\w_]+)[ \t@]*([^\n\r]*)[\n\r]+(---@param[^\r\n]+[\n\r]+)*(---@return([\w\._,\[\] \|]*)[@ \t]*([^\r\n]*)[\n\r]+)*.+(function)*[^\r\n\.:]+(\.|:)?\1([\t= ]+function)*[\t ]*\(([\w, ]*)\)]]
		--local reg = [[---([a-z]+\w+)([^\n\r]*)[\n\r]+(---param([^\r\n]+[\n\r]+))*.+\1\((.*)\)]]
		local it = gmatch(file, reg, 'jio')
		while true do
			local m, err = it()
			if not m then
				break
			end
			local name = m[1]
			funs[name] = {}
			if not is_empty(m[2]) then
				funs[name].desc = m[2]
			end

			local it2 = gmatch(m[0], [[---@param ([\w_]+)+[\t ]*([\w_\.\[\]\|]+)*[@\s]*([^@\r\n]*)[\n\r]+]], 'oji')
			if it2 then
				while true do
					local m2, err2 = it2()
					if not m2 then
						break
					end
					if not funs[name].params then
						funs[name].params = {}
					end
					local type = m2[2]
					local desc = m2[3]
					local required, default
					if is_empty(desc) then
						desc = nil
					else
						if nfind(desc, [[\[[^\]\n]*required]], 'jio') then
							required = 1
						end
						local mc = nmatch(desc, [[\[[^\]\n]*default:\s*([^\],]+)]], 'jio')
						if mc then
							if type == 'number' then
								default = tonumber(mc[1])
							else
								default = mc[1]
							end

						end
					end
					local param_name = m2[1]
					if param_name == 'self' then
						funs[name].with_self = true
					else
						ins(funs[name].params, {
							name = param_name,
							type = type,
							desc = desc,
							required = required,
							default = default
						})
					end
					--funs[name].params[m2[1]] = {
					--	type = type,
					--	desc = desc
					--}
					--dump(m2)
				end
			end
			if m[5] then
				-- Parse `return` docs
				local it3 = gmatch(m[5], [[([\w\._\[\]\|]+)]], 'jio')
				if it3 then
					while true do
						local m3, err2 = it3()
						if not m3 then
							break
						end
						if not funs[name].returns then
							funs[name].returns = {}
						end
						ins(funs[name].returns, m3[1])
					end
				end
			end
			if not is_empty(m[6]) then
				funs[name].return_desc = m[6]
			end
			if m[8] == ':' then
				funs[name].with_self = true
			end
		end
		local lua_obj = {
			source = file_name,
			funs = funs,
			ref = ''
		}
		file_name = ssub(file_name, #root + 1, #file_name - 4)
		file_name = string.gsub(file_name, '/', '.')
		local mc = nmatch(file, [[---@class ([\w_\.]+):*([\w_\.]+)*]], 'jio')
		if mc then
			-- The class is extend from base
			lua_obj.class = mc[1]
			lua_obj.base = mc[2] or nil
		else
			lua_obj.class = file_name
		end
		lua_obj.lua_name = file_name
		doc[file_name] = lua_obj
		if lua_obj.class then
			class_doc[lua_obj.class] = lua_obj
		end
	end
	for i, v in pairs(doc) do
		if v.base then
			-- Copy field from base class to current
			local base = class_doc[v.base]
			if base and base.funs then
				--dump(base)
				for fun_name, fun_desc in pairs(base.funs) do
					if not v.funs[fun_name] then
						--dump(fun_name)
						v.funs[fun_name] = fun_desc
					end
				end
			end
		end
	end
	--For tripple inheritance
	for i, v in pairs(doc) do
		if v.base then
			local base = class_doc[v.base]
			if base and base.funs then
				--dump(base)
				for fun_name, fun_desc in pairs(base.funs) do
					if not v.funs[fun_name] then
						v.funs[fun_name] = fun_desc
					end
				end
			end
		end
	end
	return doc, class_doc
end

local ref_parameter = [[(([\w\.]+)\[\])|table<[\w\.]+,([\w\.]+)>]]
local function get_refs(str, arr)
	local mc = nmatch(str, ref_parameter, 'jio')
	if not mc then
		return arr
	end
	local tp = mc[2] or mc[3]
	if tp == 'string' or tp == 'number' or tp == 'function' or tp == 'boolean' or tp == 'table' or tp == 'userdata' or tp == 'null' then
		return arr
	end
	if not arr then
		arr = {}
	end
	ins(arr, tp)
	return arr
end

local parameter_reg = [[(\w+):?(\w+[\[\]\|\w]+|table<[^>]+>)?]]
local return_reg = [[(\w+[\[\]\|\w]+|table<[^>]+>)]]
local function parse_parameter(str, is_return, refs)
	local it
	if is_return then
		it = gmatch(str, return_reg, 'joi')
	else
		it = gmatch(str, parameter_reg, 'joi')
	end
	local params, nc, with_self = {}, 1
	if it then
		while true do
			local mmc = it()
			if not mmc then
				break
			end
			if is_return then
				local type = mmc[1]
				params[nc] = type
				refs = get_refs(type, refs)
				nc = nc + 1
			else
				local param = {}
				local name = mmc[1]
				param.name = name
				if name == 'self' then
					with_self = true
				else
					refs = get_refs(mmc[2], refs)
					param.type = mmc[2]
					params[nc] = param
					nc = nc + 1
				end
			end
		end
	end
	return params, with_self, refs
end

---parse_field
---@param str string @field code
---@return string, string, string, string @ field_name, type, format, comment
local function parse_field(str, class_name)
	local inx = sfind(str, '@', 11)
	local comment
	if inx then
		comment = ssub(str, inx + 2, #str)
		inx = inx - 2
	end
	local required, default
	if nfind(comment, [[\[[^\]\n]*required]], 'jio') then
		required = 1
	end
	local mc = nmatch(comment, [[\[[^\]\n]*default:\s*([^\]\s,]+)]], 'jio')
	if mc then
		if type == 'number' then
			default = tonumber(mc[1])
		else
			default = mc[1]
		end

	end

	str = ssub(str, 1, inx or #str)
	--ngx.say(str)
	local arr = split(str, ' ')
	if arr[1] == 'private' or arr[1] == 'protected' then
		return
	elseif arr[1] == 'public' then
		table.remove(arr, 1)
	end
	local type, format = arr[2]
	if arr[3] then
		inx = sfind(str, type, 3, true)
		type = ssub(str, inx, #str)
	end
	if type and sfind(type, 'date', 1, true) then
		type = 'string'
		format = 'date-time'
	elseif type == "number" then
		format = 'int32'
	else
		local reg = [[fun\(([^\)]+)*\):?([^@]+)?]]
		local mc = nmatch(type, reg, 'jio')
		if mc then
			type = 'function'
			local params, with_self, refs, returns, _ = parse_parameter(mc[1])
			returns, _, refs = parse_parameter(mc[2], true, refs)
			format = {
				--str = mc[0],
				with_self = with_self,
				params = params,
				returns = returns,
				refs = refs
			}
		end
	end
	return arr[1], type, format, comment, default, required
end

---get_lua_doc_obj
---@param folder_list string[] @ folder to search lua files
---@param doc1 lua_doc.fun
---@param doc2 lua_doc.parameter
---@return table<string, table<string, lua_doc.parameter>>
function _M.get_lua_doc_obj(folder_list)
	local doc = {}
	--@---@field\s+[^\r\n]+[\n\r]
	local reg = [[---@class\s+([^\n\r]+)[\n\r]((---@[^\r\n]+[\n\r])+)]]
	local reg2 = [[field\s+([^\n\r]+)]]
	local flist = _M.search_file(folder_list, nil, [[\.lua$]])
	for i = 1, #flist do
		local file_name = flist[i]
		local file = utils.read_file(file_name)
		local it1 = gmatch(file, reg, 'jio')
		if it1 then
			while true do
				local smc = it1()
				if not smc then
					break
				end
				local it2 = gmatch(smc[2], reg2, 'jio')
				local obj, nc = {}, 1
				if it2 then
					local class_name = smc[1]
					local inx = sfind(class_name, ':', 2, true)
					if inx then
						class_name = ssub(class_name, 1, inx - 1)
					end
					local po = {}
					while true do
						local mc = it2()
						if not mc then
							break
						end
						local field_name, ftype, format, comment, default, required = parse_field(mc[1], class_name)
						if field_name then
							po[field_name] = {
								type = ftype,
								desc = comment,
								format = format,
								default = default,
								required = required
							}
						end
					end

					doc[class_name] = po
				end
			end
		end
	end
	return doc
end

local ignore_folder_reg = [[/(\.\w+)|(tmp|temp)|(proxy|body|cgi)[^/]*(tmp|temp|cache)/]]
---search_file search and return file lists
---@param folder_list string[]|string @Nullable means search current path
---@param file_list string[] @full file path to search
---@param whit_list_reg string @regular expression for accepted files
---@param remove_root_folder boolean @remove root folder path for outputs files list
function _M.search_file(folder_list, file_list, whit_list_reg, remove_root_folder)
	local root = root .. '/lua/'
	file.chdir(root)
	local dic, file_list = {}, {}
	if type(folder_list) == 'string' then
		folder_list = { folder_list }
	end
	for i = 1, #folder_list do
		local folder = folder_list[i]
		if not nfind(ignore_folder_reg, folder, 'jo') then
			file.foreach(folder, function(path, attr)
				if dic[path] then
					return
				end
				if whit_list_reg and not nfind(path, whit_list_reg, 'joi') then
					return
				end
				if byte(path, 1) == file.DIR_SEP_byte then
					if remove_root_folder then
						path = ssub(path, #root + 1, #path)
					end
				else
					if not remove_root_folder then
						path = root .. path
					end
				end
				dic[path] = true
				ins(file_list, path)
			end, { recurse = true })
		end
	end
	return file_list
end

local remove_directory_reg = [[(name['": =]+)[^'"]+[\\/]+]]
local remove_slash_reg = [[(\\+/*|//+)]]
local binary_bad_file_reg = [[([\\/]+\.)|(\.(obj|dll|exe|bin|so|jpg|gif|mp4|mp3|avi|png|zip|tar|gz)$)]]
function _M.get_directory(pathstr, white_list, black_list, is_raw, is_recurse)
	local sb = {}
	local is_root = false;
	if (pathstr == '' or pathstr == '/') then
		pathstr = '.'
		is_root = true
	end
	local _str = pathstr .. '/'
	file.foreach(_str,
			function(pname, attr)
				--logs(pname, mode)
				pname = nsub(pname, [[\\+]], '/', 'jo')
				if black_list then
					if nfind(pname, black_list, 'oj') then
						return
					end
				end
				if (white_list) then
					if not nfind(pname, white_list, 'oj') then
						return
					end
				end
				--if nfind(pname, binary_bad_file_reg, 'oj') then
				--	return
				--end
				local isfile = (attr.mode == 'file')
				table.insert(sb, { name = pname, isFile = isfile, path = pathstr .. '/' })
			end,
			{
				param = "fm"; -- request full path and mode
				delay = true; -- use snapshot of directory
				recurse = is_recurse; -- include subdirs
				reverse = false; -- subdirs at first
			})
	local root, list
	if #sb > 1 then
		root = ssub(sb[1].path, 2, 500)
		local full = nsub(sb[1].name, [[\\+]], '/', 'jo')
		local inx = sfind(full, root, 1, true)
		root = ssub(full, 1, inx)
	end
	if is_raw then
		return sb, root
	end
	local str = nsub(json.encode(sb), remove_directory_reg, '$1', 'jo')
	str = nsub(str, remove_slash_reg, '/', 'jo')
	return str, root
end

local class_header_template = '\n--[[{{comment}}]]\n' .. [[
---@class {{class}} @{{desc}}
local _M = { {*enum_class*}
}
]]

local field_template = '---@field {{name}} {{type}} @ {{desc}}\n'
local field_func_template = '---@field {{name}} fun({{params}}):{{returns}} @ {{desc}}\n'
local func_template = '\n--[[{{comment}}]]\n' .. [[
---{{name}} {{desc}}{{params}}
---@return {{returns}}
function {{name_prefix}}{*with_self and ':' or '.'*}{{name}}({{param_names}}){{content}}
end
]]


---@class lua_doc.format_info
---@field name string @ name of the class
---@field name_prefix string @ name of the class
---@field content string @ name of the class
---@field params string @ name of the class
---@field desc string @ name of the class
---@field comment string @ name of the class
---@field with_self boolean @ name of the class
---@field returns string @ name of the class

---get_func_kv
---@param item lua_doc.fun
local function get_func_kv(item, as_function_stub, name_prefix)
	local info = {
		name = item.name,
		desc = item.desc,
		with_self = item.with_self,
		comment = item.comment,
		returns = concat(item.returns or {}, ', '),
		name_prefix = name_prefix or '',
	}
	if as_function_stub then
		local p, names = '', ''
		for i = 1, #item.params do
			local param = item.params[i]
			p = p .. '\n---@param ' .. param.name .. ' ' .. param.type .. ' @ ' .. (param.desc or '')
			names = names .. param.name .. ', '
		end
		info.params = ssub(p, 1, -2)
		info.param_names = ssub(names, 1, -3)
	else
		local p = ''
		for i = 1, #item.params do
			local param = item.params[i]
			p = p .. param.name .. ':' .. param.type
		end
		info.params = p
	end
	return info
end

---dump_class_by
---@param doc lua_doc
function _M.dump_class_by(doc, with_codes, no_class_header)
	if not doc.name or not doc.funs then
		return nil, 'name and function list required'
	end
	local header, sub_class, docs, codes, enum_class = '', '', '', '', ''
	local class_name = doc.class or doc.name
	local name_prefix = doc.name_prefix or doc.class
	if doc.enum_class then
		enum_class = 'enum = {'
		for name, item in pairs(doc.enum_class) do
			local inx = sfind(name, '.Vips', 5, true)
			local ename = ssub(name, inx + 1, -1)
			enum_class = enum_class .. '\n---@class ' .. name .. dump_lua(item, '' .. ename)..','
		end
		enum_class = enum_class .. '}'
	end
	if not no_class_header then
		doc.enum_class = enum_class
		header = template.result(class_header_template, doc)
	end
	if doc.sub_class then
		for name, tb in pairs(doc.sub_class) do
			sub_class = sub_class .. '\n---@class ' .. name .. '\n'
			for i = 1, #tb do
				sub_class = sub_class .. template.result(field_template, tb[i])
			end

		end
	end
	if doc.fields then
		if doc.fields[1] then
			for i = 1, #doc.fields do
				local item = doc.fields[i]
				docs = docs .. template.result(field_template, item)
			end
		else
			for name, item in pairs(doc.fields) do
				if not item.name then
					item.name = name
				end
				docs = docs .. template.result(field_template, item)
			end
		end
	end
	if doc.funs[1] then
		for i = 1, #doc.funs do
			local item = doc.funs[i]
			if with_codes then
				codes = codes .. template.result(func_template, get_func_kv(item, true, name_prefix))
			else
				docs = docs .. template.result(field_func_template, get_func_kv(item, false, name_prefix))
			end
		end
	else
		for name, item in pairs(doc.funs) do
			if not item.name then
				item.name = name
			end
			if with_codes then
				codes = codes .. template.result(func_template, get_func_kv(item, true, name_prefix))
			else
				docs = docs .. template.result(field_func_template, get_func_kv(item, false, name_prefix))
			end
		end
	end
	return header .. sub_class .. docs .. codes
end

function _M.main ()
	-- file.chdir(utils.root..'/lua/app')
	-- --dump(file.chdir('../../../'))
	-- dump(file.foreach('./*.lua', function(path)
	-- 	ngx.say(path)
	-- end, { recurse = true }))
	--dump(_M.search_file('./app'))
end
return _M

---
---lua object documentation object
---@class lua_doc
---@field source string @ source code
---@field name string @ required lua path name
---@field class string @ lua doc class name
---@field funs table<string, lua_doc.fun>|lua_doc.fun @ source code
---@field fields table<string, lua_doc.parameter>|lua_doc.parameter[]
---@field comment string @ code comment above class
---@field desc string @ show simple description in class
---@field name_prefix string @ prefix ahead of the method name in generated code
---@field sub_class table|table[] @ prefix ahead of the method name in generated code
---@field enum_class table<string, table<string, string|number|boolean>> @ prefix ahead of the method name in generated code

---@class lua_doc.fun
---@field params lua_doc.type[] @ function parameters
---@field with_self boolean @ indicate whether this function should call with instance at first parameter
---@field name string @[Nullable]name of this function
---@field desc string @description for this function
---@field returns string[] @class type for returns
---@field comment string[] @class type for returns

---@class lua_doc.type
---@field name string @ description for this parameter
---@field type string @ boolean|string|number|fun|table|class
---@field desc string @ description for this parameter

---@class lua_doc.parameter
---@field type string @ boolean|string|number|fun|table|class
---@field desc string @ description for this parameter
---@field format string @ description for this parameter
