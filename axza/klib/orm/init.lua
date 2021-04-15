require('klib.common')
local utils = require('klib.utils')
local sfind, ssub, split, insert, concat, tonumber, hash, array, copy_to, type, remove, byte = string.find, string.sub, string.split, table.insert, table.concat, tonumber, table.hash, table.array, table.copy_to, type, table.remove, string.byte
local next, unpack, pcall, pairs = next, unpack, pcall, pairs
local dump, logs, dump_class, dump_lua, dump_doc, dump_dict = require('klib.dump').locally()
local tree = require('klib.orm.tree')

---@class klib.orm.init:klib.orm.base
local _M = {

}

setmetatable(_M, { __index = require('klib.orm.base') })

---new
---@param table_name string
---@param db_name string
---@param mysql_conf mysql.conf
---@return klib.orm.init
function _M.new(table_name, db_name, mysql_conf)
	if not mysql_conf and not _M.default_config then
		error('nil configs and no default mysql configurations found. Please call `set_default_config()` to inject first')
	end
	mysql_conf = mysql_conf or copy_to(_M.default_config)
	table_name = table_name or ''
	mysql_conf.connect_config.database = db_name or mysql_conf.connect_config.database
	db_name = mysql_conf.connect_config.database
	---@type klib.orm.init
	local inst = {
		conf = mysql_conf,
		_DB = db_name,
		_NAME = table_name,
	}
	setmetatable(inst, { __index = _M })
	return inst
end

function _M.set_default_config(mysql_config)
	_M.default_config = mysql_config
	_M.default_db = mysql_config.connect_config.database
	return _M
end

dump.partial(_M, require('klib.orm.tree'))
dump.partial(_M, require('klib.orm.query'))
dump.partial(_M, require('klib.orm.misc'))

return _M