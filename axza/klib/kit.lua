---
--- Created by Administrator.
--- DateTime: 2017/8/10 20:55
---
require('klib.common')
local ins = table.insert
local quote_sql = ngx.quote_sql_str
local trim = string.trim
local gsub = ngx.re.gsub
---@class kit
local kit = {}

function kit.filter_sql(sqlcmd)
	sqlcmd = trim(sqlcmd)
	if not sqlcmd then
		return nil
	end
	local itr = gsub(sqlcmd,
			[[(^|[^`'"a-z_%\$])(order|where|desc|to|asc|key|exec|count|set|select|user|use|switch|index|check|case|delete|create|update|insert|into|drop|truncate)([^`a-z_\$%'"]|$)]],
			'$1 `$2` $3', 'joi')
	return itr
end

function kit.filter_order(sqlcmd)
	sqlcmd = trim(sqlcmd)
	if not sqlcmd then
		return nil
	end
	local itr = gsub(sqlcmd,
			[[(^|[^`'"a-z_%\$])(where|exec|count|to|and|set|select|user|use|switch|index|check|case|delete|create|update|insert|into|drop|truncate)([^`a-z_\$%'"]|$)]],
			'$1 `$2` $3', 'joi')
	return itr
end

function kit.filter_where(sqlcmd)
	sqlcmd = trim(sqlcmd)
	if not sqlcmd then
		return ''
	end
	local itr = gsub(sqlcmd,
			[[(^|[^`'"a-z_%\$])(where|exec|count|to|set|select|user|use|switch|index|check|case|delete|create|update|insert|into|drop|truncate)([^`a-z_\$%'"]|$)]],
			'$1 $3', 'joi')
	return itr
end

function kit.descartes2(arr1, arr2)
	local result = {}
	for i = 1, #arr1 do
		for j = 1, #arr2 do
			ins(result, { arr1[i], arr2[j] });
		end
	end
	return result;
end

function kit.descartes3(list)
	local arr2D = kit.descartes2(list[1], list[2]);
	--dump( kit.descartes2DAnd1D(arr2D, {8}))
	for i = 3, #list do
		arr2D = kit.descartes2DAnd1D(arr2D, list[i]);
	end
	return arr2D;
end

function kit.descartes2DAnd1D(arr_2d, arr_1d)
	local result = {}
	local len = #arr_2d[1] + 1
	for j = 1, #arr_1d do
		local item = arr_1d[j]
		for i = 1, #arr_2d do
			local arrOf2D = arr_2d[i];
			if arrOf2D[len] ~= item then
				arrOf2D[len] = item
			end
			ins(result, arrOf2D);
		end
	end
	return result
end

return kit
--[[
local sb = kit:sbuffer()
sb:add('11', 'ss', 123,4343, 4343):add('324324'):add('2',4356,6565,1111):add():add(nil):add(2223232):add(false)
print(sb:tos(','))
]]
