require('klib.common')
local utils = require('klib.utils')
local datetime = require('klib.datetime')
local find, indexOf, is_datetime, sub, split, insert, concat, tonumber, hash, array, copy_to, type, is_empty, assert, tostring = string.find, string.indexOf, datetime.is_datetime_string, string.sub, string.split, table.insert, table.concat, tonumber, table.hash, table.array, table.copy_to, type, string.is_empty, assert, tostring
local next, unpack, pcall, pairs = next, unpack, pcall, pairs
local nfind, nmatch, gmatch, byte, char = ngx.re.find, ngx.re.match, ngx.re.gmatch, string.byte, string.char
local dump, logs, dump_class, dump_lua, dump_doc, dump_dict = require('klib.dump').locally()
local nfind, crc32_long, t_empty, ngsub = ngx.re.find, ngx.crc32_long, table.isempty, ngx.re.gsub
local kit = require('klib.kit')
local fsql = kit.filter_sql
local fwheresql = kit.filter_where
local forder = kit.filter_order
local quote_sql = ngx.quote_sql_str
local ins, buffer = table.insert, string.buffer

---@type klib.orm.init
local _M = {}

---exec execute the sql command in database
---@param sql string
---@return any
function _M:exec(sql)
	local db = self:get_db()
	return db:exec(sql);
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
		return nil, 'unable to delete constant data!'
	end
	local res, err = self.db:query("delete from " .. self._NAME .. " where " .. pk .. "=?", { id })
	return res, err
end

local minus_id = [[(^\-\d+$)|(^\-\d+,)|(,\-\d+$)]]
---delete_in_ids
---@param id_arr_str string @ SQL[ 1,25,545 or "xxx","3223","432432"
---@return table @ table.affect_rows
function _M:delete_in(...)
	local id_arr_str = { ... }
	if #id_arr_str == 1 then
		if id_arr_str == nil then
			return nil, 'empty input'
		end
		id_arr_str = id_arr_str[1]
		if type(id_arr_str) == 'table' then
			id_arr_str = "'" .. concat(id_arr_str, "','") .. "'"
		else
			id_arr_str = tostring(id_arr_str)
		end
	else
		id_arr_str = concat(id_arr_str, ',')
	end

	if nfind(id_arr_str, minus_id, 'jo') then
		return nil, 'unable to delete constant data!'
	end
	--id_arr_str = ngx.quote_sql_str(id_arr_str)

	local pk = self:get_PK()
	id_arr_str = kit.filter_sql(id_arr_str)
	local res, err = self.db:exec("delete from " .. self._NAME .. " where " .. pk .. " in (" .. id_arr_str .. ")")
	return res, err
end

function _M:delete_tree(node_id, result, callback)
	result = result or array(10)
	local id_val = tonumber(node_id)
	if id_val and id_val < 0 then
		return nil, 'unable to delete constant data!'
	end
	local res, err = self.db:query("delete from " .. self._NAME .. " where id =?", { node_id })
	local list = self:get_list(1, 2000, { id = 'id' }, { parent_id = node_id })
	if list and #list > 0 then
		for i = 1, #list do
			local sub_id = list[i].id
			ins(result, sub_id)
			self:delete_tree(sub_id, result)
		end
	end
	return res, err
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
		return result, err
	else
		return result[1], err
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
					return nil, 'unable to update constant data!'
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
		return nil, 'nothing to update!'
	end
	sb:pop(1)
	where = self:build_where(where)
	if not is_empty(where) then
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
		return res, err
	end
end

---batch_update
---@param po_list table[] @plain object list
---@param where string
---@param update_pk boolean
function _M:batch_update(po_list, where, update_pk)
	if not po_list or #po_list == 0 then
		return nil, 'nothing changed'
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
		return nil, 'nothing changed'
	end
	local batchsql = 'start transaction;\n' .. concat(sql_array, '') .. 'commit;'
	local res, err = self.db:exec(batchsql)
	return res, err, id_arr
end

---insert a new record
---@param obj_po table
---@param insert_pk boolean @ insert primary key within po
---@param sql_array string[] @collect insert sql, no insert sql will executed;
function _M:insert(obj_po, insert_pk, sql_array)
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
						return nil, 'bad PK value!'
					else
						if n < 0 then
							return nil, 'unable to insert constant data!'
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
		return nil, 'empty object please check the field'
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
			local pkid = res.insert_id
			if pkid then
				obj_po[pk] = pkid
			end
		end
		return res, err
	end
end

---batch_insert
---@param po_list table @po array to insert
---@param insert_pk boolean @ Nullable insert with pk
function _M:batch_insert(po_list, insert_pk, callback)
	if not po_list or #po_list == 0 then
		return nil, 'nothing changed'
	end
	local sql_array = {}
	for i = 1, #po_list do
		local po = po_list[i]
		self:insert(po, insert_pk, sql_array)
	end
	local batchsql = 'start transaction;\n' .. concat(sql_array, '') .. '\ncommit;\n'
	local res, err = self.db:exec(batchsql)
	return res, err
end

---update_by_ids
---@param id_arr table @id list array
---@param obj_po table @plain object to indicate which fields to update
---@param sql_array table @return sql command with sql_array injected
function _M:update_by_ids(id_arr, obj_po, sql_array)
	if not id_arr then
		return nil, 'empty id list!'
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
		return nil, 'nothing to update!'
	end
	sb:pop(1)
	sb:add(' WHERE `', pk.Field, '` IN( ', id_list_string(id_arr), ' )')
	--sb:add(' WHERE `', pk.Field, '` IN( ', pk_val, ' )')
	if sql_array and type(sql_array) == 'table' then
		ins(sql_array, sb:add(';\n'):tos())
		return sql_array
	else
		local res, err = self.db:exec(sb:tos())
		return res, err
	end
end

return _M