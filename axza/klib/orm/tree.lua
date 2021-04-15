require('klib.common')
local utils = require('klib.utils')
local datetime = require('klib.datetime')
local find, indexOf, is_datetime, sub, split, insert, concat, tonumber, hash, array, copy_to, type, is_empty, assert, tostring = string.find, string.indexOf, datetime.is_datetime_string, string.sub, string.split, table.insert, table.concat, tonumber, table.hash, table.array, table.copy_to, type, string.is_empty, assert, tostring
--local next, unpack, pcall, pairs = next, unpack, pcall, pairs
--local nfind, nmatch, gmatch, byte, char = ngx.re.find, ngx.re.match, ngx.re.gmatch, string.byte, string.char
--local dump, logs, dump_class, dump_lua, dump_doc, dump_dict = require('klib.dump').locally()
local DB = require('klib.db')
local kit = require('klib.kit')

---@type klib.orm.init
local _M = {}

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

---@class orm.tree.options
local default_tree_option = {
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
---@param self klib.orm.base
---@param options orm.tree.options @ Nullable
---@return klib.orm.tree_node[], klib.orm.tree.desc
function _M.get_tree(self, options)
	options = options or default_tree_option
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
				insert(root, po) -- put it into root list
			else
				local parent_obj = dic[pid] -- try to get parent
				if not parent_obj then
					-- cant find parent
					insert(root, po)
					if to_update then
						insert(change_list, { [pk] = po[pk], [parent_key] = 0 })  -- move it to root in db
					end
				else
					if not parent_obj[child_list_key] then
						parent_obj[child_list_key] = { po } -- not be parent before, create new children list for it
					else
						-- has children list, just push in
						insert(parent_obj[child_list_key], po)
					end
				end
			end
		else
			insert(root, po) -- allow nil parent_id, put it into root list
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
					insert(change_list, _uo)
				end
			else
				if _uo and to_update then
					insert(change_list, _uo)
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
						insert(change_list, { [pk] = node[pk], [order_key] = i })
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
					insert(change_list, { [pk] = node[pk], [order_key] = i })
				end
			end
		end
	end
	if #change_list > 0 then
		--ngx.log(ngx.NOTICE, '[TREE]', 'correct treenodes in [', self._DB, '.', self._NAME, ']')
		--self:batch_update(change_list)
	else
		change_list = nil
	end
	local tree_desc = { children = child_list_key, value = pk, parent_key = parent_key, label = label_key, count = count, pk = pk, name_key = name_key, path_key = path_key, has_children_key = has_children_key }
	return root, tree_desc, tree_dic, change_list
end

return _M

---@class klib.orm.tree_node
---@field id number
---@field __children base_model.tree[]
---@field lablel string
---@field parent_id number
---@field path string


---@class klib.orm.tree.desc
---@field children string
---@field value string
---@field parent_key string
---@field label string
---@field count number
---@field name_key string
---@field path_key string
---@field has_children_key string