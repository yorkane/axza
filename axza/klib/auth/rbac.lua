---
--- Created by Administrator.
--- DateTime: 2017/8/10 20:55
---
require('klib.common')
local ebitop = require('klib.base.ebitop')
local has_bit = ebitop.has_bit
local bor = ebitop.bor
local ins, is_empty, sfind, split, tostring, tonumber, upper, indexOf, has_params, sub, type = table.insert, string.is_empty, string.find, string.split, tostring, tonumber, string.upper, string.indexOf, string.has_params, string.sub, type
local tiretree = require('klib.base.tiertree')
local gsub, nfind = ngx.re.gsub, ngx.re.find
local is_regex = string.is_regex
---@class RBAC
local _M = {
	controller_dic = {},
	controller_list = {},
	---@type table<number,RBAC.role_item>
	role_dic = {},
	url_role_map = {},
	view_dic = {},
	default_host = 'root', -- all ip request will override to this host, please set this to your own domain host
	shared_space = {
		['localhost'] = true
	} --at lease one host
}

--local view_dic = {}
--local controller_dic = {}
--local controller_list = {}
--local role_dic = {}
--local url_role_map = {}

local permissionMap = { --权限数据对照字典
	[1] = 'GET | 允许 GET 访问',
	[2] = 'POST',
	[4] = 'PUT',
	[8] = 'DELETE',
	[16] = 'PATCH',
	[32] = '创建 | 允许 delete 参数',
	[64] = '编辑 | 允许 update 参数',
	[128] = '删除 | 允许 insert 参数',
}
local params = { 'delete', 'update', 'insert' }
local nums, plength = {}, #params
for i = 1, plength do
	ins(nums, math.pow(2, i + 4))
end

-----load_controller_list
-----@param url string @allow regular expression
-----@param id string|number
--local function load_controller_list(url, id, default_host, controller_list)
--	local uobj = parse_url(url, default_host)
--	local host_dic = controller_list[uobj.host]
--	if not host_dic then
--		host_dic = {}
--		controller_list[uobj.host] = host_dic
--	end
--	local root_list = host_dic[uobj.root]
--	if not root_list then
--		root_list = {}
--		host_dic[uobj.root] = root_list
--	end
--	ins(root_list, { url = url, id = id, is_regex = is_regex(url) or nil })
--end

---new
---@return RBAC
function _M:new()
	local instance = {
		controller_dic = {},
		controller_list = {},
		role_dic = {},
		url_role_map = {},
		view_dic = {}, --initialize
		controller_reg = {},
		ttree = tiretree.new(),
		shared_space = _M.shared_space
	}
	setmetatable(instance, { __index = _M })
	return instance
end

function _M:add_shared_host(host)
	if host then
		_M.shared_space[host] = true
	end
end

---build_short_url, make request ignore protocol
---@param url string @url, relative url
---@param default_host string @ append default host to relative url
function _M:build_short_url(url, default_host)
	local inx = sfind(url, '://', 1, true)
	if inx and inx < 10 then
		-- is http://xxx or https://xxx ws:// ftp:// socket://
		return sub(url, inx + 3, #url)
	end
	return default_host .. url
end

---load_menu_items
---@param list RBAC.menu_item[]
function _M:load_menu_items(list)
	local controller_dic = self.controller_dic
	local controller_list = self.controller_list
	local view_dic = self.view_dic
	if not list or #list < 0 then
		ngx.log(ngx.EMERG, 'empty menu list, please check your injection!')
		assert('empty menu list, please check your injection!')
		return
	end
	---@type RBAC.menu_item
	local item, has_controller, has_href, host_dic, root_list, uobj, href, url, id;
	for i = 1, #list do
		item = list[i]
		href, url, id = item.href, item.controller, item.id
		has_controller = not is_empty(url)
		has_href = not is_empty(href)
		if has_controller then
			--load_controller_list(url, id, self.default_host, controller_list)
			--url = _M:build_short_url(url, _M.default_host)
			if not self.ttree:insert(url, id) then
				ngx.log(ngx.WARN, 'Duplicated RBAC url found, please check your menu settings', url, ' ')
			end
			if is_regex(url) then
				self.controller_reg[id] = url
			end
			controller_dic[url] = id
		end
		if has_href then
			--href = _M:build_short_url(href, _M.default_host)
			view_dic[href] = id
			local ns = sfind(href, '#', 1, true)
			if ns ~= 1 then
				if not has_controller then
					controller_dic[href] = id -- mark href both view and controller
					--load_controller_list(href, id, self.default_host, controller_list)
					self.ttree:insert(href, id)
				end
			end
		end

	end
	return view_dic, controller_dic, controller_list
end

---register_parent
---@param role RBAC.role_item
---@param role_dic table<number, RBAC.role_item>
local function register_parent(role, role_dic)
	if role.parent_id ~= 0 and role.parent_id ~= '0' then
		local prole = role_dic[role.parent_id]
		if not prole then
			return
		end
		for menu_id, v in pairs(role.dic) do
			local permits = prole.dic[menu_id] or 0
			prole.dic[menu_id] = bor(permits, v) -- copy url role permits to parent role
		end
		local map = prole.children
		if not map then
			map = table.hash(2)
			prole.children = map
		end
		map[role.id] = true
		if role.children then
			table.copy_to(role.children, map)
		end
		register_parent(prole, role_dic)  --recursive parent role
	end
end

---register_refs
---@param role RBAC.role_item
local function register_refs(role, role_dic)
	local refs, role_id, ref_role, permits, arr = role.role_refs
	if not refs then
		return
	end
	arr = split(refs, ',')
	for i = 1, #arr do
		role_id = tonumber(arr[i])
		---@type RBAC.role_item
		ref_role = role_dic[role_id]
		if ref_role then
			for menu_id, v in pairs(ref_role.dic) do
				permits = role.dic[menu_id] or 0
				role.dic[menu_id] = bor(permits, v) -- copy refercened role's permits to current role
			end
			local map = role.children
			if not map then
				map = table.hash(2)
				role.children = map
			end
			map[ref_role.id] = true
			if role.children then
				table.copy_to(role.children, map)
			end
		end
	end
end

---load_role_items
---@param list RBAC.role_item
function _M:load_role_items(list)
	if not list or #list < 0 then
		assert('empty menu list, please check your injection!')
	end
	local role_dic = self.role_dic  --initialize
	local url_role_map = self.url_role_map

	---@type RBAC.role_item
	local role, pstr, role_refs, arr, parr, menu_id, permits, url_role_permit_dic;
	-- register leaves node roles
	for i = 1, #list do
		role = list[i]
		role.id = role.id
		role.parent_id = role.parent_id
		role.dic = {}
		role_dic[role.id] = role
		pstr = role.permission
		arr = split(pstr, ',')
		for i = 1, #arr do
			parr = split(arr[i], '|')
			menu_id = tonumber(parr[1])
			if menu_id then
				permits = tonumber(parr[2])
				role.dic[menu_id] = permits
			end
		end
	end
	-- register trunk/root node roles
	for i = 1, #list do
		role = list[i]
		register_parent(role, role_dic)  -- related to parent
		register_refs(role, role_dic) --related to reference
	end
	--role_refs = item.role_refs
	for i = 1, #list do
		role = list[i]
		register_parent(role, role_dic) --in case lose permits relations in last cycle
		register_refs(role, role_dic) --in case lose permits relations in last cycle
		for menu_id, permits in pairs(role.dic) do
			if permits > 0 then
				url_role_permit_dic = url_role_map[menu_id]
				if not url_role_permit_dic then
					url_role_map[menu_id] = { [role.id] = permits }
				else
					url_role_permit_dic[role.id] = permits -- register role_id and permits
				end
			end
		end
		role.parent_id = nil
		role.permission = nil
		role.role_refs = nil
		role.desc = nil
		role.has_children = nil
		role.order = nil
	end
	return url_role_map
end

---get_view_id
---@param url string
---@param role_id string|number
function _M:get_view_id(url)
	local view_id = self.view_dic[url]  --view url is fixed
	return view_id
end

---get_controller_id
---@param url string
---@return table
function _M:get_controller_id(url)
	--url = _M:build_short_url(url, _M.default_host)
	local controller_id = self.controller_dic[url]
	if controller_id then
		--directly hit /xxx/sss.sss
		return controller_id
	end
	local ok, word, arr = self.ttree:search(url, true)
	if word then
		--logs(url, ok, word, arr)
		return arr
	end
	return

	--try to get controller_id directly from dic
	--local controller_id = self.controller_dic[url]
	--if controller_id then
	--	--directly hit /xxx/sss.sss
	--	return { controller_id }
	--end
	--local uobj, host_dic, root_list, it, arr, host = parse_url(url, _M.default_host)
	---- try to convert ip to default_host
	--if (not utils.is_domain(uobj.host)) or _M.shared_space[uobj.host] then
	--	host = _M.default_host
	--else
	--	host = uobj.host
	--end
	--host_dic = self.controller_list[host] -- get [www.xxx.com] dic
	--if host_dic then
	--	root_list = host_dic[uobj.root] or host_dic['/'] -- get [www.xxx.com][/xxxx/] list
	--	if root_list then
	--		for i = 1, #root_list do
	--			it = root_list[i]
	--			if it.is_regex then
	--				if nfind(url, it.url, 'jo') then
	--					if not arr then
	--						arr = { it.arr }
	--					else
	--						ins(arr, it.id)
	--					end
	--				end
	--			elseif sfind(url, it.url, 1, true) then
	--				--match /xxxx/xxxx.xxx/xxxx to [/xxxx/]
	--				if not arr then
	--					arr = { it.id }
	--				else
	--					ins(arr, it.id)
	--				end
	--			end
	--		end
	--	end
	--end
	--return arr
end

local function has_params_permission(url, permits)
	for i = 1, plength do
		local pm, num = params[i], nums[i]
		if has_params(url, pm) then
			if has_bit(permits, num) then
				return true
			else
				return false, 403.3
			end
		end
	end
	-- not match any parameter
	return true
end

function _M:has_view_permission(url, role)
	if is_empty(url) then
		return true
	end
	local view_id, controller_arr, vdic, cdic, permits, has_view, has_controller, code = self:get_view_id(url)
	if not view_id then
		-- no url matched to the permission control, release to GO
		return true
	end
	if view_id then
		vdic = self.url_role_map[view_id]
	end
	--role = tostring(role)
	if vdic then
		-- is view url
		permits = vdic[role]
		-- to visit view only required read
		if permits and has_bit(permits, 1) then
			return true
		end
		return false, 403.2 -- no permits for this url and controller neither
	end
	-- could not find in views, no url match user owned permissions
	return false, 403.1
end

---has_permission relative paths required domain for indicator in first load
---@param url string @ url to check, full or partial both accepted
---@param role number @role id to check
---@param method string @http method
---@param level number @level optional
function _M:has_permission(url, role, method, level)
	if is_empty(url) then
		return true
	end
	--dump(url, self.controller_dic)
	--url = _M:build_short_url(url, _M.default_host)
	--require("mobdebug").start("127.0.0.1", 8172)
	local view_id, controller_arr, vdic, cdic, permits, has_view, has_controller, code = self:get_view_id(url), self:get_controller_id(url)
	-- not registered in view and controller url dic
	if not view_id and not controller_arr then
		-- no url matched to the permission control, release to GO
		return true
	end
	local url_role_map = self.url_role_map

	if view_id then
		vdic = url_role_map[view_id]
	end
	if controller_arr then
		if type(controller_arr) == 'table' then
			cdic = {}
			for i = 1, #controller_arr do
				local item = url_role_map[controller_arr[i]]
				ins(cdic, item)
			end
		else
			local item = url_role_map[controller_arr]
			if item then
				cdic = { item }
			end
		end
	end
	--logs(url, view_id, vdic, controller_arr, cdic)

	--role = tostring(role)
	if vdic then
		-- is view url
		permits = vdic[role]
		-- to visit view only required read
		if permits and has_bit(permits, 1) then
			return true
		end
		return false, 403.2 -- no permits for this url and controller neither
	else
		-- could not find in views and controllers, no url match user owned permissions
		if not cdic then
			return false, 403.1
		end
	end

	-- is controller url
	for i = 1, #cdic do
		local dic, flag = cdic[i]
		--Any controller permission matched will be granted
		permits = dic[role]
		if permits then
			if method == 'GET' and has_bit(permits, 1) then
				flag, code = has_params_permission(url, permits)
				if flag then
					return true
				end
			end
			if method == 'POST' and has_bit(permits, 2) then
				flag, code = has_params_permission(url, permits)
				if flag then
					return true
				end
			end
			if method == 'PUT' and has_bit(permits, 4) then
				flag, code = has_params_permission(url, permits)
				if flag then
					return true
				end
			end
			if method == 'DELETE' and has_bit(permits, 8) then
				flag, code = has_params_permission(url, permits)
				if flag then
					return true
				end
			end
			if method == 'PATCH' and has_bit(permits, 16) then
				flag, code = has_params_permission(url, permits)
				if flag then
					return true
				end
			end
			code = code or 405
		end
	end
	-- user's role don't has any permissions on this url, return last code
	return false, code or 403.1
end

---get_role
---@param role_id number|string
---@return RBAC.role_item
function _M:get_role(role_id)
	return self.role_dic[role_id]
end

---has_role
---@param role_id_parent number @the role item to detect if has child_id
---@param role_id_child number
function _M:has_role(role_id_parent, role_id_child)
	local role1 = self.role_dic[role_id_parent]
	local map = role1.children
	if map then
		return map[role_id_child]
	end
end

function _M.test()
	local rbac = _M:new()
	local role_list = {
		[1] = {
			id = 101,
			parent_id = 0,
			permission = "1|255",
			role_refs = "0"
		},
		[2] = {
			id = 102,
			parent_id = 1,
			permission = "1|255",
			role_refs = "0"
		},
		[3] = {
			id = 103,
			parent_id = 1,
			permission = "",
			role_refs = "0"
		},
		[4] = {
			id = 104,
			parent_id = 2,
			permission = "",
			role_refs = "0"
		},
		[5] = {
			id = 105,
			parent_id = 2,
			permission = "",
			role_refs = "0"
		},
		[6] = {
			id = 106,
			parent_id = 3,
			permission = "",
			role_refs = "0"
		},
	}
	local menu_list = {
		[1] = {
			href = "/count1/",
			controller = "/post1",
			id = 201,
		},
		[2] = {
			href = "/count2/",
			controller = "/post2",
			id = 202,
		},
		[3] = {
			href = "/count3/",
			controller = "/post3",
			id = 203,
		},
		[4] = {
			href = "/count4/",
			controller = "/post4",
			id = 204,
		},
		[5] = {
			href = "/count5/",
			controller = "/post5",
			id = 205,
		},
		[6] = {
			href = "/count6/",
			controller = "/post6",
			id = 206,
		},
	}
	rbac:load_menu_items(menu_list)
	rbac:load_role_items(role_list)
end

return _M

---@class RBAC.menu_item
---@field id number
---@field href string @ view url
---@field controller string @ controller url

---@class RBAC.role_item
---@field id number @role_id
---@field permission string @permissions mapped to the urls
---@field role_refs string @inherits to other roles
---@field parent_id number @the parent role will get all permissions from this role
---@field dic table<number, RBAC.role_item>
---@field children table<number,boolean>
