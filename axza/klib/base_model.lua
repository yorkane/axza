---
--- Created by Administrator.
--- DateTime: 2017/8/10 20:55
---
require('klib.common')
local kit = require('klib.kit')
local DB = require('klib.db')
local datetime = require('klib.datetime')
local ins, buffer = table.insert, string.buffer
local quote_sql = ngx.quote_sql_str
local nmatch = ngx.re.match
local nfind, crc32_long, t_empty, ngsub = ngx.re.find, ngx.crc32_long, table.isempty, ngx.re.gsub
local find, indexOf, is_datetime, sub, split, insert, concat, tonumber, hash, array, copy_to, type, is_empty, assert, tostring = string.find, string.indexOf, datetime.is_datetime_string, string.sub, string.split, table.insert, table.concat, tonumber, table.hash, table.array, table.copy_to, type, string.is_empty, assert, tostring
local fsql = kit.filter_sql
local fwheresql = kit.filter_where
local forder = kit.filter_order
local sql_reduce_map = {}
local _splitter = '' --char(127)
local TAG = '[BASE_MODEL]'
---@type cache_bus
local cache_bus = {
	get = function()
	end,
	set = function()
	end,
	delete = function()
	end
}
---@type event_bus
local event_bus

---@class base_model
---@field public _FIELD_LIST base_model.field[] @ Map to table columns
---@field public _FIELD_DIC table<string, base_model.field> @ Table Columns map to dic
---@field public db DB @ databse DAL object
---@field public _NAME string @ Table Name in database
---@field public _DB string @ databse name
---@field public _PK string @ primary key
---@field public _version string @ DDL version of the table, md5 hash
---@field public default_config mysql.conf
local _M = {
	default_config = {  -- mysql配置
		--timeout = 5000,
		--connect_config = {
		--	host = "127.0.0.1",
		--	port = 3306,
		--	database = "app",
		--	charset = "utf8",
		--	user = "root",
		--	password = "root",
		--	max_packet_size = 1024 * 1024
		--},
		--pool_config = {
		--	max_idle_timeout = 20000, -- keep alive timeout 20s
		--	pool_size = 20 -- mysql connection pool size
		--}
	}
}

function _M.enable_cache_event()
	cache_bus = require('klib.biz.cache_bus')
	event_bus = require('klib.biz.event_bus')
end

function _M.set_config(mysql_config)
	_M.default_config = mysql_config
	_M.default_db = mysql_config.connect_config.database
	--logs(_M.default_db, mysql_config)
end

---get_db
---@param dname string,
---@param mysql table @ customized mysql configuration
---@return DB
function _M.get_db(dname, tname, mysql)
	--logs(_M.default_config, tname,dname)
	---@type mysql.conf
	mysql = mysql or copy_to(_M.default_config, {})
	--logs(dname, tname, mysql, _M.default_config, appconf.mysql.connect_config)
	tname = tname or ''
	--logs(_M.default_config, mysql, mysql.connect_config, '======>', tname, dname)
	mysql.connect_config.database = dname or mysql.connect_config.database
	dname = mysql.connect_config.database
	local db = DB:new(mysql, '[DB:' .. dname .. '.' .. tname .. ']')
	db._DB = dname
	--logs(_M.default_config, tname,dname)
	return db
end

---get _FIELD_LIST from model
---@param model base_model @Nullable
---@return base_model.field[]
function _M:get_field_list(model)
	model = model or self
	if model._FIELD_LIST then
		return model._FIELD_LIST
	end
	-- get table description from database
	local res, err = model.db:exec('show create table ' .. model._NAME)
	if err then
		return nil, err
	end
	local sql = res[1]['Create Table']
	local table_comment = nmatch(sql, [[Comment[^=]*=[^'"`]*[`'"](.+?)[`'"]+]], 'joi')
	if table_comment and table_comment[1] then
		model._COMMENT = table_comment[1]
	end
	local start = find(sql, 'ENGINE=', 1, true)
	sql = sub(sql, 1, start - 1)
	model._version = ngx.md5(sql)
	res, err = model.db:exec('show full columns from ' .. model._NAME)
	if err then
		return nil, err
	end
	local default, pk_field = {}
	for i = 1, #res do
		local fi = res[i]
		-- Only the first primary key marked as _PK
		if not pk_field and indexOf(fi.Key, 'PRI') == 1 then
			fi.isPK = true
			model._PK = fi.Field
			pk_field = fi
		end
		fi.isUNI = indexOf(fi.Key, 'UNI') == 1 and true or nil

		local mc = nmatch(fi.Type, [[\((\d+)\)]], 'jo')
		fi.width = mc and tonumber(mc[1]) or 10000
		fi.notNull = (fi['Null'] ~= 'YES') and 1 or nil;
		fi['Null'] = nil

		fi.isDate = nfind(fi.Type, [[(date|time)]], 'jo') and true or nil
		fi.isBool = indexOf(fi.Type, 'bit') == 1 and true or nil
		if (not fi.isOption) and (indexOf(fi.Type, 'int') > 0) then
			fi.isInt = true
		end

		if indexOf(fi.Type, 'tinyint(1)') == 1 then
			if nmatch(fi.Field, [[(^[sS]how[A-Z_]|^[Hh]ide[A-Z_]|^[Hh]as[A-Z_]|^[nN]ot?[A-Z_]|^[Ii]s[A-Z_]|^[eE]nable[A-Z_]|^[dD]isable[A-Zd_]|^[aA]llow[A-Zd_]|\w+[F_f]+lag$)]], 'jo') then
				fi.isIntBool = true
				fi.isInt = nil
			end
		end

		if indexOf(fi.Type, 'smallint(5)') == 1 or indexOf(fi.Type, 'enmu') == 1 then
			fi.isOption = true;
			fi.isInt = nil;
		end

		if indexOf(fi.Type, 'smallint(4)') == 1 then
			fi.isStatus = true;
			fi.isInt = nil;
		end
		if indexOf(fi.Type, 'char') > 0 or indexOf(fi.Type, 'text') > 0 then
			fi.isText = true;
		end
		if indexOf(fi.Field, 'option_') == 1 then
			fi.isTextOption = true;
			fi.isText = nil;
		end
		fi.isBlob = indexOf(fi.Type, 'blob') > 0 and true or nil

		if nmatch(fi.Type, [[float|double]]) then
			fi.isInt = nil;
			fi.isFloat = true;
		end
		if fi.Default then
			if type(fi.Default) == 'userdata' or find(fi.Default, 'TIMESTAMP', 1, true) then
				fi.Default = nil
			end
			if fi.Default == '' then
				fi.Default = nil
			end
			if fi.isInt or fi.isIntBool or fi.isStatus or fi.isOption or fi.isFloat then
				local n = tonumber(fi.Default)
				if n then
					if fi.isIntBool and n == 0 then
						fi.Default = nil
					else
						fi.Default = n
					end
				else
					fi.Default = nil
				end
			end
			if fi.Default then
				default[fi.Field] = fi.Default
			end
		end

		--str = tostring(fi.Collation)
		--if find(str, 'userdata: NULL') then
		--	fi.Collation = nil
		--else
		--	fi.Collation = str
		--end

		if fi.Key == '' then
			fi.Key = nil
		end
		if fi.Field == 'parent_id' then
			if fi.width == pk_field.width and (fi.isInt == pk_field.isInt and fi.isText == fi.isText) then

				model._IS_TREE = true
			end
		end
		--if fi.Extra == '' then
		if is_empty(fi.Comment) then
			fi.Comment = nil
		end
		fi.Extra = nil
		--end
		fi.Privileges = nil
		fi.Collation = nil
		fi.Type = nil
		if (fi.isIntBool or fi.isBool or fi.isDate) then
			fi.width = nil
		end
	end
	model._FIELD_LIST = res
	model._DEFAULT = default
	local db, key = model.db, 'model.' .. model._DB .. '.' .. model._NAME

	cache_bus.set(key, {
		_NAME = model._NAME,
		_DB = model._DB,
		_FIELD_LIST = model._FIELD_LIST,
		_DEFAULT = model._DEFAULT,
		_COMMENT = model._COMMENT,
		_version = model._version,
		_PK = model._PK
	}, 300)
	return res
end

---check_model check the model is valid to schema
---@param obj_po table
function _M:check_model(obj_po, is_insert)
	if not obj_po then
		return false, 'Empty Data', 1
	end
	for i, val in pairs(obj_po) do
		local fi = self:get_field(i)
		if fi then
			if not val and (fi.notNull == 1) and is_insert then
				return false, i.Comment .. '|' .. fi.Field .. ': Could not empty', 1
			end
			if fi.isFloat or fi.isInt or fi.isOption or fi.isIntBool or fi.isStatus then
				if type(val) ~= 'number' then
					return false, fi.Comment .. '|' .. fi.Field .. ': Must be number', 2
				end
			end
			if fi.isText and #val > fi.width then
				return false, fi.Comment .. '|' .. fi.Field .. ': too long', 3
			end
		end
	end
	return true
end

function _M:query(sql)
	--return self.db:exec(sql) --disable it due to security reasons
end

---is_equals compare 2 plain model object is the same, base on the field value
---@param po1 table
---@param po2 table
function _M:is_equals(po1, po2)
	if po1 == po2 then
		return true
	end
	if po1 == nil or po2 == nil then
		return
	end
	local list, err, key = self:get_field_list()
	if err then
		return nil, err
	end
	for i = 1, #list do
		local field = list[i]
		key = field.Field
		if po1[key] ~= po2[key] then
			if field.isBool then
				if (po1[key] == nil and po2[key] == false) or (po1[key] == false and po2[key] == nil) then
					--empty value is accepted
					return true
				end
			elseif field.isIntBool then
				if (po1[key] == nil and po2[key] == 0) or (po1[key] == 0 and po2[key] == nil) then
					--empty value is accepted
					return true
				end
			end
		end
	end
	return true
end

---serialize
---@param instance table
---@param splitter string|nil
---@return string @serialized string
function _M:serialize(instance, splitter)
	if not instance then
		return
	end
	if type(instance) == 'string' then
		return instance
	end
	splitter = splitter or _splitter
	local list, err, val = self:get_field_list()
	if err then
		return nil, err
	end
	local arr = array(#list)
	for i = 1, #list do
		local fi = list[i]
		if not fi.width then
			val = instance[fi.Field] or ''
			insert(arr, val)
		elseif fi.width < 201 then
			-- ignore too large text
			val = instance[fi.Field] or ''
			insert(arr, val)
		end
	end
	return concat(arr, splitter)
end
---deserialize
---@param instance_str string
---@param splitter string|nil @ separator for serialized string
---@return table|nil @ instance
function _M:deserialize(instance_str, splitter)
	if not instance_str then
		return
	end
	if type(instance_str) == 'table' then
		return instance_str
	end
	splitter = splitter or _splitter
	local arr, val = split(instance_str, splitter, 1, true)
	if not arr or #arr < 3 then
		--at least 2 field in instance
		return nil
	end
	local list, instance = self:get_field_list()
	for i = 1, #list do
		val = arr[i]
		if val then
			if not instance then
				instance = {}
			end
			local fi = list[i]
			--field conversion
			if fi.isIntBool then
				instance[fi.Field] = (tonumber(val) == 1 and 1 or nil)
			elseif fi.isIntBool or fi.isInt or fi.isStatus or fi.isOption or fi.isFloat then
				instance[fi.Field] = tonumber(val)
			else
				instance[fi.Field] = val ~= '' and val or nil
			end
		end
	end
	return instance
end
---get_sort_closure
---@param order_key1 string @field1 to sort
---@param order_key2 string @sort on field2 when field1 equals field2
---@return boolean @ true = switch, false = unchange
local function get_sort_closure(order_key1, order_key2)
	if not order_key1 or order_key1 == '' then
		return nil, 'Key is empty'
	end
	return function(node1, node2)
		local v1, v2 = tonumber(node1[order_key1]) or 1, tonumber(node2[order_key1]) or 2
		local v = v2 - v1
		if v > 0 then
			return true
		else
			if v == 0 then
				if order_key2 then
					v1, v2 = tonumber(node1[order_key2]) or 1, tonumber(node2[order_key2]) or 2
					return v2 - v1 > 0
				end
			end
			return false
		end
	end
end

---get_count
---@param where string @ SQL:[ `x1` = 'xxxx'  or `x2` > 1 oder by `x1` DESC
---@return number
function _M:get_count(where)
	where = self:build_where(where)
	local pk = self:get_PK()
	local res, err = self.db:query("select count(" .. pk .. ") as c from " .. self._NAME .. where)
	if err or not res or #res ~= 1 or not res[1].c then
		return 0, err
	else
		return tonumber(res[1].c)
	end
end

function _M:build_where(where)
	if not where or where == '' then
		return ''
	end
	local tp = type(where)
	if tp == 'string' then
		if find(where, 'WHERE', 1, true) then
			-- has been built, no need to filter again
			return where
		end
		where = fwheresql(where)
		if not where then
			return ''
		end
		local new_where, replaced_count, err = ngsub(where, [[([\w`]+)([ >=<!]+\d+)]], function(m)
			local name = m[1]
			local fi = self:get_field(name)
			if fi.notNull then
				return m[0]
			else
				return 'IFNULL(' .. m[1] .. ',0)' .. m[2]
			end
		end, "joi")
		return ' WHERE ' .. new_where
	end
	if tp ~= 'table' then
		return ''
	end
	local sb = buffer(' WHERE ')
	for name, val in pairs(where) do
		local inx = find(name, '%[')
		if inx then
			name = sub(name, 1, inx - 1)
		end
		local fi = self:get_field(name)  -- only table column field to update
		local tp = type(val)
		if fi and val and tp ~= 'userdata' then
			if tp == 'number' then
				if fi.notNull then
					sb:add(' `', fi.Field, '` = ', val, ' AND ')
				else
					sb:add(' IFNULL(`', fi.Field, '`, 0) = ', val, ' AND ')
				end

				--elseif fi.isIntBool or fi.isOption or fi.isStatus then
				--	sb:add(' `', fi.Field, '` = ', val, ' AND ')
			elseif fi.isDate then
				local d1, d2 = val[1], val[2]
				if is_datetime(d1) and is_datetime(d2) then
					--sb:add(' `', fi.Field, '` > FROM_UNIXTIME(', val
					--, ') AND `', fi.Field, '` < FROM_UNIXTIME(', val, ') ', 'AND')
					if d1 and d2 then
						sb:add(' `', fi.Field, '` > "', d1
						, '" AND `', fi.Field, '` < "', d2, '" ', ' AND ')
					end
				end
			else
				val = kit.filter_sql(val)
				if val then
					if find(val, '%%') then
						sb:add(' `', fi.Field, '` LIKE "', val, '"', ' AND ')
					elseif nfind(val, [[^[ ]*(([><= ]+['"\w]+)|(is null)|(is not null))[ ]*$]], 'joi') then
						sb:add(' `', fi.Field, '` ', val, ' AND ')
					elseif nfind(val, [[^[ ]*IFNULL\([\w, ]+\)[\!\=\'\"\w ]+]], 'joi') then
						sb:add(val, ' AND ')
					else
						sb:add(' `', fi.Field, '` = "', val, '"', ' AND ')
					end
				end
			end
		end
	end
	return sb:pop(1):tos()
end

---delete_by_id
---@param id string|number
---@return number @ row deleted
function _M:delete(id)
	local pk = self:get_PK()
	local id_val = tonumber(id)
	if id_val and id_val < 0 then
		return { err = 'unable to delete constant data!' }
	end
	local res, err = self.db:query("delete from " .. self._NAME .. " where " .. pk .. "=?", { id })
	if not err and event_bus then
		event_bus.emit('base_model/delete', self._NAME, id, self._PK, self._DB)
	end
	return { result = res, err = err }
end

local minus_id = [[(^\-\d+$)|(^\-\d+,)|(,\-\d+$)]]
---delete_in_ids
---@param id_arr_str string @ SQL[ 1,25,545 or "xxx","3223","432432"
---@return table @ table.affect_rows
function _M:delete_in(...)
	local id_arr_str = { ... }
	if #id_arr_str == 1 then
		if id_arr_str == nil then
			return { err = 'empty input' }
		end
		id_arr_str = id_arr_str[1]
		if type(id_arr_str) == 'table' then
			id_arr_str = "'" .. concat(id_arr_str, "','").."'"
		else
			id_arr_str = tostring(id_arr_str)
		end
	else
		id_arr_str = concat(id_arr_str, ',')
	end

	if nfind(id_arr_str, minus_id, 'jo') then
		return { err = 'unable to delete constant data!' }
	end
	--id_arr_str = ngx.quote_sql_str(id_arr_str)

	local pk = self:get_PK()
	id_arr_str = kit.filter_sql(id_arr_str)
	local res, err = self.db:exec("delete from " .. self._NAME .. " where " .. pk .. " in (" .. id_arr_str .. ")")
	if not err and event_bus then
		event_bus.emit('base_model/delete_in', self._NAME, id_arr_str, self._PK, self._DB)
	end
	return { result = res, err = err }
end

function _M:delete_tree(node_id, result)
	result = result or array(10)
	local id_val = tonumber(node_id)
	if id_val and id_val < 0 then
		return { err = 'unable to delete constant data!' }
	end
	local res, err = self.db:query("delete from " .. self._NAME .. " where id =?", { node_id })
	if not err and event_bus then
		--event_bus.emit('base_model/delete', self._NAME, node_id, pk, self._DB)
	end
	local list = self:get_list(1, 2000, { id = 'id' }, { parent_id = node_id })
	if list and #list > 0 then
		for i = 1, #list do
			local sub_id = list[i].id
			ins(result, sub_id)
			self:delete_tree(sub_id, result)
		end
	end
	return result
end

---get_by_id
---@param id string|number @id to select from table
function _M:get_by_id(id)
	if not id then
		return nil, 'empty id'
	end
	local pk = self:get_PK()
	local result, err = self.db:query('select * from ' .. self._NAME .. ' where ' .. pk .. '=?', { id })
	if not result or err or type(result) ~= "table" or #result ~= 1 then
		return nil, err
	else
		return result[1], err
	end
end

local function id_list_string(id_arr)
	local str
	if type(id_arr[1]) == 'number' then
		str = concat(id_arr, ',')
	else
		str = '"' .. concat(id_arr, '","') .. '"'
	end
	return str
end

---get_by_ids
---@param id string[]|number[] @id to select from table, and order by array index
---@param is_explain boolean @ just get the sql command, No DB execution
function _M:get_by_ids(id_arr, is_explain)
	if type(id_arr) == 'string' then
		id_arr = split(id_arr, ',')
	end
	local pk, str = self:get_PK(), id_list_string(id_arr)
	local cmd = 'select * from ' .. self._NAME .. ' where '
			.. pk .. ' in (' .. str .. ') order by find_in_set(' .. pk .. ',"' .. concat(id_arr, ',') .. '")'
	if is_explain then
		return cmd
	end
	local result, err = self.db:exec(cmd)
	if not result or err or type(result) ~= "table" or #result ~= 1 then
		return { result = result, err = err }
	else
		return { result = result[1], err = err }
	end
end

---where get the single plain object from DB
---@param where table|string
---@param custom_obj table @Nullable customized plain-object {id = 'cc', name = 'label'} redefine the column name
---@param is_explain boolean @ just get the sql command, No DB execution
---@return table|string @ single object
function _M:where(where, order_by, custom_obj, is_explain)
	where = self:build_where(where)
	if order_by then
		order_by = ' ORDER BY ' .. forder(order_by)
	else
		order_by = ''
	end
	local sql = buffer('SELECT ')
	if custom_obj and type(custom_obj) == 'table' then
		for k, v in pairs(custom_obj) do
			k = fsql(k)
			v = fsql(v)
			if k then
				if type(v) == 'string' then
					sql:add(' ', k, ' as `', v, '` ', ',')
				else
					sql:add(' ', k, ',')
				end
			end
		end
		sql:pop()
	else
		sql:add('*')
	end
	sql:add(' FROM ', self._NAME, where, order_by, " LIMIT 1")
	if is_explain then
		return sql:tos()
	end
	local res, err = self.db:exec(sql:tos())
	return res[1], err
end

---get_list
---@param page number @default 1 page to start 1, 2, 4
---@param page_size number @default 20 page size 20, 30, 50,
---@param custom_obj table @{id = 'cc', name = 'label'} redefine the column name
---@param where table|string @SQL `xx` > 1 or `xx2` = 'dummy'
---@param orderby string @order by `field`table
---@param is_explain boolean @ just get the sql command, No DB execution
---@return table[], string, string @result list, error message, sql command
function _M:get_list(page, page_size, custom_obj, where, orderby, is_explain)
	page = tonumber(page) or 1
	if page < 1 then
		page = 1
	end
	page_size = tonumber(page_size) or 20
	local nums = (page - 1) * page_size
	orderby = forder(orderby)
	where = self:build_where(where)
	orderby = orderby and (' ORDER BY ' .. orderby) or (' ORDER BY ' .. self:get_PK() .. ' DESC ')
	local sql = buffer('SELECT ')
	if custom_obj and type(custom_obj) == 'table' then
		for k, v in pairs(custom_obj) do
			k = fsql(k)
			v = fsql(v)
			if k then
				if type(v) == 'string' and k ~= v then
					sql:add(' ', k, ' as ', v, ',')
				else
					sql:add(' ', k, ',')
				end
			end
		end
		sql:pop()
	else
		sql:add('*')
	end
	sql:add(' FROM ', self._NAME, where, orderby, " LIMIT ", nums, ' , ', page_size)
	local cmd = sql:tos()
	if is_explain then
		return cmd
	end
	local res, err = self.db:exec(cmd)
	return res, err, cmd
end

---update primary key will not update!
---@param obj_po table
---@param where string|table @ empty where will be ignored
---@param update_pk boolean @force to update primary key
---@param sql_array table @collect update sql, no update sql will executed;
---@return table
function _M:update(obj_po, where, update_pk, sql_array)
	local sb = buffer('update '):add(self._NAME):add(' set ')
	---@type base_model.field
	local pk, pk_val, has_change, pk_value
	for name, val in pairs(obj_po) do
		local fi = self:get_field(name)  -- only table column field to update
		if fi then
			if fi.isPK then
				pk = fi
				pk_value = val
				pk_val = quote_sql(val)
				if fi.isInt and tonumber(val) < -1 then
					return { err = 'unable to update constant data!' }
				end
				if update_pk then
					sb:add(' `', fi.Field, '` = ', pk_val, ',')
				end
			else
				if val ~= nil then
					if fi.isText and type(val) == 'table' then
						val = concat(val, ',')
					end
					if fi.isDate then
						local dt = val
						if not dt.utc then
							dt = datetime.new(val)
						end
						sb:add(' `', fi.Field, '` = "', dt, '"', ',')
					else
						sb:add(' `', fi.Field, '` = ', quote_sql(val), ',')
					end
					has_change = true
				end
			end
		end
	end
	if not has_change then
		return { err = 'nothing to update!' }
	end
	sb:pop(1)
	where = self:build_where(where)
	if not string.is_empty(where) then
		--sb:add(' WHERE ', where)
		sb:add(where)
	elseif pk_val then
		sb:add(' WHERE `', self._PK, '` = ', pk_val)
	end
	if sql_array and type(sql_array) == 'table' then
		ins(sql_array, sb:add(';\n'):tos())
		return sql_array
	else
		local res, err = self.db:exec(sb:tos())
		if not err and event_bus then
			local cp = self.default_config.cache_bus[self._NAME]
			if cp and cp ~= 0 then
				if cp == 1 then
					event_bus.emit('base_model/update', self._NAME, pk_value or '-100', self._PK, self._DB)
				elseif not t_empty(cp) then
					for i, v in pairs(obj_po) do
						if cp[i] == 1 then
							event_bus.emit('base_model/update', self._NAME, pk_value or '-100', self._PK, self._DB)
							break
						end
					end
				end
			end
			--logs("event_bus.emit('base_model/update')", self._NAME, pk_val, self._PK, self._DB)
		end
		return { result = res, err = err }
	end
end

---batch_update
---@param po_list table[] @plain object list
---@param where string
---@param update_pk boolean
function _M:batch_update(po_list, where, update_pk)
	if not po_list or #po_list == 0 then
		return { err = 'nothing changed' }
	end
	where = self:build_where(where)
	local sql_array = array(#po_list)
	local id_arr = array(#po_list)
	local pk = self:get_PK()
	for i = 1, #po_list do
		local po = po_list[i]
		self:update(po, where, update_pk, sql_array)
		id_arr[i] = po[pk]
	end
	if #sql_array < 1 then
		return { err = 'nothing changed' }
	end
	local batchsql = 'start transaction;\n' .. concat(sql_array, '') .. 'commit;'
	local res, err = self.db:exec(batchsql)
	if not err and event_bus then
		event_bus.emit('base_model/batch_update', self._NAME, concat(id_arr, ','), self._PK, self._DB)
	end
	--log(res, err)
	return { result = res, err = err }
end

---update primary key will not update!
---@param self base_model
---@param obj_po table
---@param insert_pk boolean @ insert primary key within po
---@param sql_array string[] @collect insert sql, no insert sql will executed;
function _M.insert(self, obj_po, insert_pk, sql_array)
	local cols = buffer('insert into '):add(self._NAME):add(' ( ')
	local values = buffer(') VALUES ( ')
	local pk, pk_val, has_change = self:get_PK()
	for name, val in pairs(obj_po) do
		local fi = self:get_field(name)

		if fi then
			if fi.isPK then
				if insert_pk and fi.isInt then
					local n = tonumber(val)
					if not n then
						return { err = 'bad PK value!' }
					else
						if n < 0 then
							return { err = 'unable to insert constant data!' }
						end
					end
				end
				pk_val = quote_sql(val)
				if insert_pk and val then
					has_change = true
					cols:add(' `', pk, '` ', ',')
					values:add(pk_val, ',')
				end
			else
				local val = obj_po[fi.Field]
				if val ~= nil then
					has_change = true
					cols:add(' `', fi.Field, '` ', ',')
					if fi.isDate then
						local dt = val
						if not dt.utc then
							dt = datetime.new(val)
						end
						values:add('"' .. dt:tostring() .. '"', ',')
					else
						values:add(quote_sql(tostring(val)), ',')
					end
				end
			end
		end
	end
	if not has_change then
		return { err = 'empty object please check the field' }
	end
	cols:pop(1) -- remove last comma
	values:pop(1):add(')') --remove last comma

	local sql = cols:tos() .. values:tos()
	if sql_array and type(sql_array) == 'table' then
		ins(sql_array, sql .. ';\n')
		return sql_array
	else
		local res, err = self.db:exec(sql)
		if not err then
			if insert_pk then
				obj_po[pk] = res.insert_id
			end
			if event_bus and (not find(self._NAME, '_log')) then
				local cp = self.default_config.cache_bus[self._NAME]
				if cp == 1 or not t_empty(cp) then
					event_bus.emit('base_model/insert', self._NAME, res.insert_id, self._PK, self._DB)
				end
			end
		end
		return { result = res, err = err }
	end
end

---batch_insert
---@param po_list table @po array to insert
---@param insert_pk boolean @ Nullable insert with pk
function _M:batch_insert(po_list, insert_pk)
	if not po_list or #po_list == 0 then
		return { err = 'nothing changed' }
	end
	local sql_array = {}
	for i = 1, #po_list do
		local po = po_list[i]
		self:insert(po, insert_pk, sql_array)
	end
	local batchsql = 'start transaction;\n' .. concat(sql_array, '') .. '\ncommit;\n'
	local res, err = self.db:exec(batchsql)
	if not err and event_bus then
		event_bus.emit('base_model/batch_insert', self._NAME, self._PK, self._DB)
	end
	return { result = res, err = err }
end

---get_field
---@param name string
---@return base_model.field
function _M:get_field(po_property_name)
	if not po_property_name then
		return nil
	end
	local dic = self._FIELD_DIC
	if not dic then
		dic = {}
		local flist, err = self:get_field_list()
		if err then
			return nil, err
		end
		for i = 1, #flist do
			---@type base_model.field
			local fi = flist[i]
			dic[fi.Field] = fi
		end
		self._FIELD_DIC = dic
	end
	return dic[po_property_name]
end

---get_PK
---@return string
function _M:get_PK()
	if not self._PK then
		local list, err = self:get_field_list()
		assert(err == nil, err)
	end
	return self._PK
end

---load_model
---@param model base_model
local function refresh_model(is_premature, model)
	if not model.db then
		model.db = _M.get_db(model._DB or _M.default_db, model._NAME)
	end
	model._FIELD_LIST = nil
	model._FIELD_DIC = nil
	_M.get_field_list(model, model)
end

---get_model
---
---@param table_name string @ Nullable
---@param db_name string @ Nullable
---@return base_model
function _M:get_model(table_name, db_name, is_ignore_cache)
	if not table_name then
		return nil, 'empty table or database'
	end
	--logs(db_name, _M.default_config, table_name)
	db_name = db_name or _M.default_db
	local key, ok, model, is_expired = 'model.' .. table_name

	if not is_ignore_cache then
		ok, model = pcall(require, key) -- try local model import
		if ok and type(model) == 'table' then
			return model
		end
	end
	if not is_ignore_cache then
		--logs(table_name, db_name, is_ignore_cache, key, _M.default_config)
		key = 'model.' .. db_name .. '.' .. table_name
		model, is_expired = cache_bus.get(key, true)
		-- keep cache warm
		if is_expired then
			ngx.timer.at(0, refresh_model, model, model)
		end
	end
	model = model or {
		_NAME = table_name,
		_DB = db_name,
	}

	return _M:init(model)
end

---@param po table
---@param po_dic table
---@param parent_key string
---@param name_key string @ columen field for
local function __get_node_path(po, po_dic, parent_key, name_key, separator)
	local path = po[name_key]
	local pid = po[parent_key]
	while (true) do
		local _po = po_dic[pid]
		if not _po then
			break
		end
		pid = _po[parent_key]
		path = _po[name_key] .. (separator or '/') .. path
		if pid == 0 then
			break
		end
	end
	return path
end

---@class base_model.tree.option
local _tree_option = {
	pk = 'id', -- Nullable primary key 'id' for po
	parent_key = 'parent_id', -- Nullable parent key 'parent_id' for po
	update_tree = false, -- Nullable refresh the tree structure when querying. CAUTION! `where` option will make structure update unpredictable!!!
	has_children_key = 'has_children', -- Nullable indicate has_children field to update, SET NIL TO IGNORE UPDATE HAS_CHILDER FIELD!
	child_list_key = '__children', -- Nullable only change when update_tree valid children id list as "12,32,43,54,65"
	path_key = 'path', -- Nullable indicate path column field to update or just append path string into object without update
	path_separator = '/', -- separator to join each path, default `/`
	name_key = 'name', -- Nullable indicate 'name' for po, use it for path crusier
	order_key = 'order', -- Nullable indicate order index for node, SET NIL TO IGNORE ORDER SORTING!
	label_key = 'label', -- Nullable indicate 'label' key text display field
	where = 'is_deleted = 0 AND is_visible = 1', -- Nullable will load all node
	max_count = 2000
}

---get_tree
---@param options base_model.tree.option @ Nullable
---@return base_model.tree[], base_model.tree.desc
function _M:get_tree(options)
	options = options or {}
	if options.where then
		options.update_tree = nil
	end
	local where = self:build_where(options.where)
	local max_count = options.max_count or 2000

	local sql = string.format("select * from `" .. self._NAME .. '` ' .. where .. ' limit ' .. max_count)
	local po_list, err = self.db:exec(sql)
	if err then
		return nil, nil, nil, err
	end
	local pk = options.pk or 'id'
	if not self:get_field(pk) then
		return nil, nil, nil, 'no matched field for primary key'
	end
	local parent_key = options.parent_key or 'parent_id'
	if not self:get_field(parent_key) then
		return nil, nil, nil, 'no matched field for parent key'
	end
	local label_key = options.label_key or 'name'
	if not self:get_field(label_key) then
		return nil, nil, nil, 'no matched field for node label text'
	end
	local name_key = options.name_key or label_key or 'name'
	if not self:get_field(name_key) then
		return nil, nil, nil, 'no matched field for node name'
	end
	local has_children_key = options.has_children_key
	if has_children_key and (not self:get_field(has_children_key)) then
		has_children_key = nil
		--return nil, nil, 'invalid field for has children key'
	end
	local child_list_key = options.child_list_key or '__children'

	local order_key = options.order_key
	if order_key and (not self:get_field(order_key)) then
		order_key = nil
		--return nil, nil, 'invalid field for has order index key'
	end
	local path_key = options.path_key
	--if path_key and (not self:get_field(path_key)) then
	--	path_key = nil
	--	--return nil, nil, 'invalid field for has path key'
	--end
	local sort
	if order_key then
		sort = get_sort_closure(order_key, pk)
	end
	local count, root, dic, parent_arr, po = #po_list, {}, {}, {}
	-- register in DIC
	for i = 1, count do
		po = po_list[i]
		local id = po[pk]
		if po[child_list_key] or not id then
			local err = 'Conflict with tree ruls: MUST HAVE ' .. pk .. ', MUST NOT HAVE __children column'
			ngx.log(ngx.ERR, '[TREE]', err)
			return nil, nil, nil, err
		end
		dic[id] = po
	end

	local to_update = options.update_tree
	local change_list = {}  -- for correcting tree node in fly
	for i = 1, count do
		po = po_list[i]
		local pid = po[parent_key]
		if pid then
			-- has value
			if pid == 0 or pid == '' or pid == '0' then
				-- allow parent_id empty or 0
				ins(root, po) -- put it into root list
			else
				local parent_obj = dic[pid] -- try to get parent
				if not parent_obj then
					-- cant find parent
					ins(root, po)
					if to_update then
						ins(change_list, { [pk] = po[pk], [parent_key] = 0 })  -- move it to root in db
					end
				else
					if not parent_obj[child_list_key] then
						parent_obj[child_list_key] = { po } -- not be parent before, create new children list for it
					else
						-- has children list, just push in
						ins(parent_obj[child_list_key], po)
					end
				end
			end
		else
			ins(root, po) -- allow nil parent_id, put it into root list
		end
	end
	local tree_dic = {}
	for i = 1, count do
		local po = po_list[i]
		tree_dic[po[pk]] = po
		local _uo
		if has_children_key and to_update then
			if po[child_list_key] then
				if po[has_children_key] == 0 then
					_uo = { [pk] = po[pk], [has_children_key] = 1 }
				end
			else
				if po[has_children_key] == 1 then
					_uo = { [pk] = po[pk], [has_children_key] = 0 }
				end
			end
		end
		--if child_list_key and to_update then
		--	local val = ''
		--	if po[child_list_key] then
		--		local arr = {}
		--		for i = 1, #po[child_list_key] do
		--			ins(arr, po[child_list_key][i][pk])
		--		end
		--		val = concat(arr, ',')
		--	end
		--	if (po[child_list_key] == nil and val == '') then
		--
		--	elseif po[child_list_key] ~= val then
		--		ins(change_list, { [pk] = po[pk], [child_list_key] = val })
		--	end
		--end
		if path_key then
			local separator = options.path_separator
			local path = __get_node_path(po, dic, parent_key, name_key, separator)
			if path ~= po[path_key] then
				po[path_key] = path
				if to_update then
					_uo = _uo or { [pk] = po[pk] }
					_uo[path_key] = path
					ins(change_list, _uo)
				end
			else
				if _uo and to_update then
					ins(change_list, _uo)
				end
			end
		end
		if order_key and po[child_list_key] then
			table.sort(po[child_list_key], sort)
			if to_update then
				for i = 1, #po[child_list_key] do
					local node = po[child_list_key][i]
					if node[order_key] ~= i then
						node[order_key] = i
						ins(change_list, { [pk] = node[pk], [order_key] = i })
					end
				end
			end
		end
	end
	if order_key then
		table.sort(root, sort)
		if to_update then
			for i = 1, #root do
				local node = root[i]
				if node[order_key] ~= i then
					node[order_key] = i
					ins(change_list, { [pk] = node[pk], [order_key] = i })
				end
			end
		end
	end
	if #change_list > 0 then
		ngx.log(ngx.NOTICE, '[TREE]', 'correct treenodes in [', self._DB, '.', self._NAME, ']')
		self:batch_update(change_list)
	end
	local tree_desc = { children = child_list_key, value = pk, parent_key = parent_key, label = label_key, count = count, pk = pk, name_key = name_key, path_key = path_key, has_children_key = has_children_key }
	return root, tree_desc, tree_dic
end

---update_by_ids
---@param id_arr table @id list array
---@param obj_po table @plain object to indicate which fields to update
---@param sql_array table @return sql command with sql_array injected
function _M:update_by_ids(id_arr, obj_po, sql_array)
	if not id_arr then
		return { err = 'empty id list!' }
	end
	---@type base_model.field
	local pk, pk_val, has_change = self:get_PK()
	local sb = buffer('update '):add(self._NAME):add(' set ')
	for name, val in pairs(obj_po) do
		local fi = self:get_field(name)  -- only table column field to update
		if fi then
			if not fi.isPK then
				if val ~= nil then
					has_change = true
					sb:add(' `', fi.Field, '` = ', quote_sql(val), ',')
				end
			end
		end
	end
	if not has_change then
		return { err = 'nothing to update!' }
	end
	sb:pop(1)
	sb:add(' WHERE `', pk.Field, '` IN( ', id_list_string(id_arr), ' )')
	--sb:add(' WHERE `', pk.Field, '` IN( ', pk_val, ' )')
	if sql_array and type(sql_array) == 'table' then
		ins(sql_array, sb:add(';\n'):tos())
		return sql_array
	else
		local res, err = self.db:exec(sb:tos())
		return { result = res, err = err }
	end
end

---get_status predefined status from sys_status table for common use, this is a static function
---@param options base_model.status.options @ Nullable,  DEFAULT GET ALL isStatus FIELD FROM sys_status, without desciption and label_en
---@return base_model.status[]
function _M:get_status(options)
	options = options or {}
	local db, dic, fi, res, err = _M.get_db(self.db.conf.sys_status_db or self._DB), {}
	local idarr = options.id
	local field = options.field or 'status'
	local gparr = options.group
	local flist, err = self:get_field_list()
	if err then
		return nil, err
	end
	if (not gparr and not idarr) or options.has_default then
		--get default status field by own table structure
		gparr = {}
		for i = 1, #flist do
			fi = flist[i]
			if fi.isStatus then
				ins(gparr, fi.Field)
			end
		end
		if #gparr == 0 then
			gparr = nil
		end
	end

	if not gparr and not idarr then
		return nil, 'no matched status field'
	end
	local bool = options.has_label
	bool = (bool == nil or bool == true) and true or false -- empty equals true,
	local sb, sql = buffer('SELECT `id` '
	, bool and ',`label` ' or ''
	, options.has_desc and ',`desc`' or ''
	, options.has_label_en and ',`label_en`' or ''
	, ',`group` FROM `', fsql(options._NAME) or 'sys_status', '` ')
	sql = sb:tos()
	sb = buffer()
	if idarr then
		local idv
		if type(idarr) == 'table' then
			--is id array {1,2,3,'4'}
			sb:add('WHERE id in(')
			for i = 1, #idarr do
				idv = idarr[i]
				sb:add(quote_sql(idv), ',')
				if type(idv) == 'number' then
					--Automatically get  minues number id
					if idv < 100 then
						-- the id < 100 could have minus number value status
						idv = idv * -1
						sb:add(idv, ',')  --add minus id into select
					end
				end
			end
			sb:pop():add(')')
		else
			sb:add('WHERE id = ', quote_sql(idarr))
		end
		sb:add(' ORDER BY `id`')

		res, err = db:exec(sql .. sb:tos())

		if err then
			return nil, err .. ' | could bad id array or bad group array, eg: {id = {1,3,4,5}, group={"base", "service"}}'
		end
		for i = 1, #res do
			res[i].group = nil -- remove unecessary field
		end
		dic[field] = res  -- default status entry for current table
	end
	sb = buffer()
	if gparr then
		if type(gparr) == 'table' then
			if #gparr == 1 then
				sb:add('WHERE `group` = ', quote_sql(gparr[1]))
			else
				sb:add('WHERE `group` in(')
				for i = 1, #gparr do
					sb:add(quote_sql(gparr[i]), ',')
				end
				sb:pop():add(')')
			end
		else
			sb:add('WHERE `group` = ', quote_sql(gparr))
		end
		sb:add(' ORDER BY `id`')
		res, err = db:exec(sql .. sb:tos())
		if err then
			return nil, err .. ' | could bad id array or bad group array, eg: {id = {1,3,4,5}, group={"base", "service"}}'
		end
		for i = 1, #res do
			fi = res[i]
			if not dic[fi.group] then
				dic[fi.group] = { fi }
			else
				ins(dic[fi.group], fi)
			end
			fi.group = nil -- remove unecessary field
		end
	end
	return dic
end

---_get_option_name toggle field name with table
---@param field_name string @ input table.field or field
---@return string @ return table.field or field
local function _get_option_name (field_name, table_name)
	local indx = find(field_name, '.', 1, true)
	if indx and indx > 0 then
		return sub(field_name, indx + 1, 100)
	end
	return table_name .. '.' .. field_name
end

---get_status predefined status from sys_status table for common use, this is a static function
---@param mapfield_list table @ Nullable {'event', 'sys_log.level'}, DEFAULT BY SELF.TABLE STRUCTURE
---@param has_desc boolean @ Nullable contains description for options, DEFAULT FALSE
---@return base_model.options
function _M:get_options(mapfield_list, has_desc)
	if not mapfield_list then
		local flist, err = self:get_field_list()
		if err then
			return nil, err
		end
		mapfield_list = array(4)
		for i = 1, #flist do
			local fi = flist[i]
			if fi.isOption then
				ins(mapfield_list, fi.Field)
			elseif fi.isTextOption then
				local name = sub(fi.Field, 8, 100)
				ins(mapfield_list, name)
			end
		end
	end
	if #mapfield_list < 1 then
		return nil, 'no option field in  this table'
	end
	local db, err, res, field, list, fi, common_field, has_match_field = _M.get_db(self.db.conf.sys_options_db or self._DB)
	list, err = db:exec('SELECT DISTINCT mapfield FROM sys_options;')
	if err then
		return nil, err
	end
	local opt_dic = hash(#list)
	for i = 1, #list do
		fi = list[i]
		opt_dic[fi.mapfield] = 1
	end
	local sb = buffer('SELECT `id`, `label`, `value`, `mapfield` ',
			has_desc and ', `desc`' or '',
			' FROM sys_options WHERE mapfield in ( ')
	for i = 1, #mapfield_list do
		field = mapfield_list[i]
		if opt_dic[field] then
			--matched the field in dic
			sb:add(quote_sql(field), ',')
			has_match_field = true
		else
			-- try to match the app."event" part field for common options
			-- Or add table.field to match in sys_options table
			field = _get_option_name(field, self._NAME)
			--logs(field)
			if opt_dic[field] then
				sb:add(quote_sql(field), ',')
				has_match_field = true
			end
		end
	end
	if not has_match_field then
		return nil, 'no matched field'
	end
	sb:pop():add(' )')
	res, err = db:exec(sb:tos())
	if err then
		return nil, err
	end

	-- dic['event'] = {option1, option2, option3}
	local dic = {}
	for i = 1, #res do
		fi = res[i]
		field = fi.mapfield
		if not dic[field] then
			dic[field] = {}
			ins(dic[field], fi)
		else
			ins(dic[field], fi)
		end
		fi.mapfield = nil
	end
	return dic
end

---get_field_info generate extra field infos from `sys_field_info` table
function _M:get_field_info()
	local db_name = self.db.conf.sys_field_info_db or self._DB
	local db = _M.get_db(db_name)
	local res, err = db:exec(" select * from sys_field_info where _NAME = '" .. self._NAME .. "' and _DB = '" .. self._DB .. "'")
	if res and not err then
		for i = 1, #res do
			local item = res[i]
			local fi = self:get_field(item.Field)
			if fi then
				fi.id = item.id
				fi.Comment = item.Comment or fi.Comment
				fi.notNull = fi.notNull or item.notNull
				fi.info = item.info
				fi.validate_regex = item.validate_regex
				fi.validate_error = item.validate_error
				fi.grid_width = item.grid_width
				fi.is_readonly = item.is_readonly
				fi.hide_in_form = item.hide_in_form
				fi.hide_in_grid = item.hide_in_grid
			end
		end
	end
	return self._FIELD_LIST
end

function _M:get_description(status_option, opt_options)
	if not self._FIELD_LIST then
		self:get_field_list()
	end
	local key1 = 'sys_options/description/' .. self._NAME
	local key2 = 'sys_status/description/' .. self._NAME
	local field_info_key = 'sys_field_info/description/' .. self._NAME
	local val1 = cache_bus.get(key1)
	if not val1 then
		val1 = self:get_status(status_option)
		cache_bus.set(key1, val1)
	end
	local val2 = cache_bus.get(key2)
	if not val2 then
		val2 = self:get_options(opt_options)
		cache_bus.set(key2, val2)
	end
	local field_info = cache_bus.get(field_info_key)
	if not field_info then
		field_info = self:get_field_info()
		cache_bus.set(field_info_key, field_info)
	end
	local empty_arr = array(1)
	local desc = {
		_DB = self._DB,
		_NAME = self._NAME,
		_COMMENT = self._COMMENT,
		_PK = self._PK,
		_FIELD_LIST = field_info,
		_DEFAULT = self._DEFAULT,
		_STATUS = val1 or empty_arr,
		_OPTIONS = val2 or empty_arr,
		_IS_TREE = self._IS_TREE
	}
	return desc
end

---cache_by_id
---@param cache_seconds number
---@param ignore_worker_cache boolean
---@param id number|string @identifier for this data
---@return table, string, string @ data, key, error
function _M:cache_by_id(cache_seconds, ignore_worker_cache, id)
	if not id then
		return
	end
	local key, str, ok = '@' .. self._NAME .. '.' .. id .. '.po'
	local po = cache_bus.get(key, ignore_worker_cache)
	if po then
		if ignore_worker_cache then
			po = self:deserialize(po)
		end
		if not po then
			cache_bus.delete(key)
			return
		else
			return po, key
		end
	end
	ok, po = pcall(self.get_by_id, self, id)
	if not ok then
		ngx.log(ngx.ERR, po)
		local err = po
		if find(po, 'API disabled', 1, true) then
			ngx.timer.at(0, _M.cache_by_id, self, cache_seconds, ignore_worker_cache, id)
			po = cache_bus.get(key, true, ignore_worker_cache)
			if po then
				return self:deserialize(po), key, err
			end
		end
		return nil, key, err
	end
	if po then
		if ignore_worker_cache then
			str = self:serialize(po)
			cache_bus.set('@' .. self._NAME .. '.' .. id .. '.po', str, cache_seconds, true)
		else
			cache_bus.set('@' .. self._NAME .. '.' .. id .. '.po', po, cache_seconds)
		end
		return po, key
	end
end

---cache_by_ids
---@param cache_seconds number
---@param ignore_worker_cache boolean
---@param id_arr_str string
---@param map_key_field string
---@param is_failover boolean @Nullable failover for this cache
---@return table[], string, boolean, string @list, key,is_expired, error
function _M:cache_by_ids(cache_seconds, ignore_worker_cache, id_arr_str, map_key_field, is_failover)
	local key = '$' .. self._NAME .. '/cache_by_ids/' .. id_arr_str
	local list, is_expired = cache_bus.get(key, ignore_worker_cache, is_failover)
	if list and not is_expired then
		return list, key
	end
	local ok, res = pcall(self.get_by_ids, self, id_arr_str)
	if not ok then
		ngx.log(ngx.ERR, res)
		local err = res
		if find(res, 'API disabled', 1, true) then
			ngx.timer.at(0, _M.cache_by_ids, self, cache_seconds, ignore_worker_cache, id_arr_str, map_key_field, is_failover)
			if not is_failover then
				list, is_expired = cache_bus.get(key, true)
				if list then
					return list, key, is_expired, err
				end
			end
		end
		return list, key, is_expired, err
	end
	if not res.err then
		list = res.result
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
			cache_bus.set(key, list, cache_seconds, ignore_worker_cache, is_failover)
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
		list, is_expired = cache_bus.get(key, cache_seconds, is_failover)
		if list and not is_expired then
			return list, key
		end
	end
	page = page or 1
	page_size = page_size or 200
	local cmd = self:get_list(page, page_size, custom_obj, where, orderby, true)
	key = '$' .. self._NAME .. '/cache_list/' .. crc32_long(cmd)
	local is_ok, res, err = pcall(self.db.exec, self.db, cmd)
	if not is_ok then
		err = res
		ngx.log(ngx.ERR, list)
		if find(err, 'API disabled', 1, true) then
			if not is_failover then
				-- try to get stale cache
				list, is_expired = cache_bus.get(key, true)
				if list then
					return list, key, is_expired, err
				end
			end
			ngx.timer.at(0, _M.cache_list, self, cache_seconds, key, page, page_size, custom_obj, where, orderby, is_failover)
		end
		return list, key, is_expired, err
	end
	if not err and res and type(res) == 'table' then
		cache_bus.set(key, res, cache_seconds, false, is_failover)
		ngx.log(ngx.INFO, TAG, key, ' set into cache ', cache_seconds)
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
		map, is_expired = cache_bus.get(key, cache_seconds, false, is_failover)
		if map and not is_expired then
			return map, key
		end
	end
	page = page or 1
	page_size = page_size or 200
	local cmd = self:get_list(page, page_size, custom_obj, where, orderby, true)
	key = '$' .. self._NAME .. '/cache_map/' .. map_key_field .. '/' .. crc32_long(cmd)
	local is_ok, list, err = pcall(self.db.exec, self.db, cmd)
	if not is_ok then
		err = list
		ngx.log(ngx.ERR, list)
		if find(list, 'API disabled', 1, true) then
			ngx.timer.at(0, function()
				self:cache_map(cache_seconds, key, map_key_field, page, page_size, custom_obj, where, orderby, is_failover)
			end)
			if not is_failover then
				-- try to get stale cache
				list, is_expired = cache_bus.get(key, true)
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
			cache_bus.set(key, nmap, cache_seconds, false, is_failover)
			--logs(map)
			ngx.log(ngx.INFO, TAG, key, ' set into cache ', cache_seconds)
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
	if not key then
		if map_key_field then
			key = '$' .. self._NAME .. '/cache_sql/map/' .. map_key_field .. '/' .. crc32_long(sql_str)
		else
			key = '$' .. self._NAME .. '/cache_sql/list/' .. crc32_long(sql_str)
		end
	end
	local map, is_expired
	map = cache_bus.get(key, cache_seconds, false, true)
	if map and not is_expired then
		return map, key
	end

	local is_ok, list, err = pcall(self.db.exec, self.db, sql_str)
	if not is_ok then
		err = list
		ngx.log(ngx.ERR, err)
		if find(list, 'API disabled', 1, true) then
			ngx.timer.at(0, function()
				self:cache_sql(cache_seconds, key, map_key_field, sql_str, is_failover)
			end)
		end
		if not is_failover then
			local nmap, n_is_expired = cache_bus.get(key, true)
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
			cache_bus.set(key, nmap, cache_seconds, false, is_failover)
			ngx.log(ngx.INFO, TAG, key, ' set into cache ', cache_seconds)
			return nmap, key, false
		else
			return map, key, is_expired, 'map_key_field not match any SQL field to result'
		end
	else
		ngx.log(ngx.INFO, TAG, key, ' set into cache ', cache_seconds)
		cache_bus.set(key, list, cache_seconds, false, is_failover)
		return list, key, false
	end
end

---init
---@param model table @table name within database
---@param db_name string @database schema
---@param mysql_config table @customized mysql connection inject
---@return base_model
function _M:init(model, db_name, mysql_config)
	setmetatable(model, { __index = _M })
	if not model.db then
		model.db = _M.get_db(db_name or model._DB or _M.default_db, model._NAME, mysql_config)
		model._DB = model.db._DB
	end
	--model.init = nil
	--model.get_model = nil
	return model
end

return _M


---@class base_model.status.options
---@field public id string @ status id to select
---@field public group string @ group to select
---@field public has_desc boolean @ add desc field
---@field public has_label_en boolean @ add label_en field
---@field public has_label_en boolean @ when group is nil, append default status groups by isStatus field
--local status_options = {
--	_NAME = 'sys_status', --default table source,
--	field = 'status',
--	id = { 1, 2, 3, '4' }, --id list to fetch this equals {1,-1,2,-2,3,-3,4} --string input id will be ignored minus value
--	group = { 'base', 'service' },
--	has_label = true, --defualt true
--	has_desc = false, -- contains `desc` field
--	has_label_en = false, -- contains `label_en` field
--	has_default = false -- include default groups when id, group are not defined
--}

---@class base_model.status
---@field id number
---@field label string
---@field desc string
---@field label_en string
--local __status = {
--	id = 200, label = '成功', desc = '请求成功', label_en = 'OK'
--}

---@class base_model.options
---@field id number
---@field label string
---@field desc string
---@field value number
--local __options = {
--	id = 200, label = 'ERROR', desc = '错误事件，但不影响系统运行', value = 4
--}



---@class base_model.tree
---@field id number
---@field __children base_model.tree[]
---@field lablel string
---@field parent_id number
---@field path string


---@class base_model.tree.desc
---@field children string
---@field value string
---@field parent_key string
---@field label string
---@field count number
---@field name_key string
---@field path_key string
---@field has_children_key string

---@class base_model.field
---@field Field string
---@field Comment string
---@field width number
---@field isBool boolean
---@field isIntBool boolean
---@field isDate boolean
---@field isInt boolean
---@field isFloat boolean
---@field isPK boolean
---@field isUNI boolean
---@field isOption boolean
---@field isText boolean
---@field isBlob boolean
---@field isStatus boolean
---@field notNull boolean
