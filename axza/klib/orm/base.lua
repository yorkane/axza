require('klib.common')
local utils = require('klib.utils')
local datetime = require('klib.datetime')
local find, indexOf, is_datetime, sub, split, insert, concat, tonumber, hash, array, copy_to, type, is_empty, assert, tostring = string.find, string.indexOf, datetime.is_datetime_string, string.sub, string.split, table.insert, table.concat, tonumber, table.hash, table.array, table.copy_to, type, string.is_empty, assert, tostring
local next, unpack, pcall, pairs = next, unpack, pcall, pairs
local nfind, nmatch, gmatch, byte, char = ngx.re.find, ngx.re.match, ngx.re.gmatch, string.byte, string.char
local dump, logs, dump_class, dump_lua, dump_doc, dump_dict = require('klib.dump').locally()
local DB = require('klib.db')
local kit = require('klib.kit')
local _splitter = char(1)

---@class klib.orm.base
---@field _NAME string
---@field _DB string
---@field _FIELD_LIST
---@field _DEFAULT table<string, string|number>
---@field _version string
---@field _PK string
---@field db DB
local _M = {
	_NAME = nil,
	_DB = nil,
	_FIELD_LIST = nil,
	_DEFAULT = nil,
	_COMMENT = nil,
	_version = nil,
	_PK = nil
}

function _M:get_db()
	local db, err = self.db
	if db then
		return db
	end
	db, err = DB:new(self.conf, '[DB:' .. self._DB .. '.' .. self._NAME .. ']')
	if err then
		return nil, err
	end
	db._DB = self._DB
	self.db = db
	return db
end

---get_field_list _FIELD_LIST from model
---@return klib.orm.field[]
function _M:get_field_list()
	if self._FIELD_LIST then
		return self._FIELD_LIST
	end
	local db = self:get_db()
	-- get table description from database
	local res, err = self.db:exec('show create table ' .. self._NAME)
	if err then
		return nil, err
	end
	local sql = res[1]['Create Table']
	local table_comment = nmatch(sql, [[Comment[^=]*=[^'"`]*[`'"](.+?)[`'"]+]], 'joi')
	if table_comment and table_comment[1] then
		self._COMMENT = table_comment[1]
	end
	local start = find(sql, 'ENGINE=', 1, true)
	sql = sub(sql, 1, start - 1)
	self._version = ngx.md5(sql)
	local db = self:get_db()
	res, err = self.db:exec('show full columns from ' .. self._NAME)
	if err then
		return nil, err
	end
	local default, pk_field = {}
	for i = 1, #res do
		local fi = res[i]
		-- Only the first primary key marked as _PK
		if not pk_field and indexOf(fi.Key, 'PRI') == 1 then
			fi.isPK = true
			self._PK = fi.Field
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

				self._IS_TREE = true
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
	self._FIELD_LIST = res
	self._DEFAULT = default
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

---get_PK
---@return string
function _M:get_PK()
	if not self._PK then
		local list, err = self:get_field_list()
		assert(err == nil, err)
	end
	return self._PK
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

-----load_model
-----@param model base_model
--local function refresh_model(is_premature, model)
--	if not model.db then
--		model.db = _M.get_db(model._DB or _M.default_db, model._NAME)
--	end
--	model._FIELD_LIST = nil
--	model._FIELD_DIC = nil
--	_M.get_field_list(model, model)
--end

return _M

---@class klib.orm.field
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