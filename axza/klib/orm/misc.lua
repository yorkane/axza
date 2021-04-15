require('klib.common')
local ins, buffer = table.insert, string.buffer
local datetime = require('klib.datetime')
local find, indexOf, is_datetime, sub, split, insert, concat, tonumber, hash, array, copy_to, type, is_empty, assert, tostring = string.find, string.indexOf, datetime.is_datetime_string, string.sub, string.split, table.insert, table.concat, tonumber, table.hash, table.array, table.copy_to, type, string.is_empty, assert, tostring
local kit = require('klib.kit')
local fsql = kit.filter_sql
local fwheresql = kit.filter_where
local forder = kit.filter_order
local quote_sql = ngx.quote_sql_str

---@type klib.orm.init
local _M = {}

---get_status predefined status from sys_status table for common use, this is a static function
---@param options base_model.status.options @ Nullable,  DEFAULT GET ALL isStatus FIELD FROM sys_status, without desciption and label_en
---@return base_model.status[]
function _M:get_status(options)
	options = options or {}
	local db, dic, fi, res, err = self:get_db(), {}
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

---get_description
---@param status_option table
---@param opt_options table
function _M:get_description(status_option, opt_options)
	if not self._FIELD_LIST then
		self:get_field_list()
	end
	local empty_arr = array(1)
	local desc = {
		_DB = self._DB,
		_NAME = self._NAME,
		_COMMENT = self._COMMENT,
		_PK = self._PK,
		_FIELD_LIST = self:get_field_info(),
		_DEFAULT = self._DEFAULT,
		_STATUS = self:get_status(status_option) or empty_arr,
		_OPTIONS = self:get_options(opt_options) or empty_arr,
		_IS_TREE = self._IS_TREE
	}
	return desc
end

return _M

---@class klib.orm.status.options
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

---@class klib.orm.status
---@field id number
---@field label string
---@field desc string
---@field label_en string
--local __status = {
--	id = 200, label = '成功', desc = '请求成功', label_en = 'OK'
--}
