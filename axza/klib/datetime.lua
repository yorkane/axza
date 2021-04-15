--- Origin code from https://daurnimator.github.io/luatz
--- https://github.com/daurnimator/luatz
--- MIT License

local local_time_zone = 0
local function _init_timezone()
	local time1 = ngx.localtime()
	local time2 = ngx.utctime()
	if time1 ~= time2 then
		local reg = [[\d[Tt ](\d+):\d+]]
		time1 = tonumber(ngx.re.match(time1, reg, 'jo')[1])
		time2 = tonumber(ngx.re.match(time2, reg, 'jo')[1])
		local_time_zone = time1 - time2
	end
end
_init_timezone()

local strformat, sbyte, sub = string.format, string.byte, string.sub
local floor, type = math.floor, type
local idiv
do
	-- Try and use actual integer division when available (Lua 5.3+)
	local idiv_loader = (loadstring or load)([[return function(n,d) return n//d end]], "idiv") -- luacheck: ignore 113
	if idiv_loader then
		idiv = idiv_loader()
	else
		idiv = function(n, d)
			return floor(n / d)
		end
	end
end

local mon_lengths = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 }
-- Number of days in year until start of month; not corrected for leap years
local months_to_days_cumulative = { 0 }
for i = 2, 12 do
	months_to_days_cumulative[i] = months_to_days_cumulative[i - 1] + mon_lengths[i - 1]
end
-- For Sakamoto's Algorithm (day of week)
local sakamoto = { 0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4 };

local function is_leap(y)
	if (y % 4) ~= 0 then
		return false
	elseif (y % 100) ~= 0 then
		return true
	else
		return (y % 400) == 0
	end
end

local function month_length(m, y)
	if m == 2 then
		return is_leap(y) and 29 or 28
	else
		return mon_lengths[m]
	end
end

local function leap_years_since(year)
	return idiv(year, 4) - idiv(year, 100) + idiv(year, 400)
end

local function day_of_year(day, month, year)
	local yday = months_to_days_cumulative[month]
	if month > 2 and is_leap(year) then
		yday = yday + 1
	end
	return yday + day
end

local function day_of_week(day, month, year)
	if month < 3 then
		year = year - 1
	end
	return (year + leap_years_since(year) + sakamoto[month] + day) % 7 + 1
end

local function borrow(tens, units, base)
	local frac = tens % 1
	units = units + frac * base
	tens = tens - frac
	return tens, units
end

local function carry(tens, units, base)
	if units >= base then
		tens = tens + idiv(units, base)
		units = units % base
	elseif units < 0 then
		tens = tens + idiv(units, base)
		units = (base + units) % base
	end
	return tens, units
end

-- Modify parameters so they all fit within the "normal" range
local function normalise(year, month, day, hour, min, sec)
	-- `month` and `day` start from 1, need -1 and +1 so it works modulo
	month, day = month - 1, day - 1

	-- Convert everything (except seconds) to an integer
	-- by propagating fractional components down.
	year, month = borrow(year, month, 12)
	-- Carry from month to year first, so we get month length correct in next line around leap years
	year, month = carry(year, month, 12)
	month, day = borrow(month, day, month_length(floor(month + 1), year))
	day, hour = borrow(day, hour, 24)
	hour, min = borrow(hour, min, 60)
	min, sec = borrow(min, sec, 60)

	-- Propagate out of range values up
	-- e.g. if `min` is 70, `hour` increments by 1 and `min` becomes 10
	-- This has to happen for all columns after borrowing, as lower radixes may be pushed out of range
	min, sec = carry(min, sec, 60) -- TODO: consider leap seconds?
	hour, min = carry(hour, min, 60)
	day, hour = carry(day, hour, 24)
	-- Ensure `day` is not underflowed
	-- Add a whole year of days at a time, this is later resolved by adding months
	-- TODO[OPTIMIZE]: This could be slow if `day` is far out of range
	while day < 0 do
		month = month - 1
		if month < 0 then
			year = year - 1
			month = 11
		end
		day = day + month_length(month + 1, year)
	end
	year, month = carry(year, month, 12)

	-- TODO[OPTIMIZE]: This could potentially be slow if `day` is very large
	while true do
		local i = month_length(month + 1, year)
		if day < i then
			break
		end
		day = day - i
		month = month + 1
		if month >= 12 then
			month = 0
			year = year + 1
		end
	end

	-- Now we can place `day` and `month` back in their normal ranges
	-- e.g. month as 1-12 instead of 0-11
	month, day = month + 1, day + 1

	return year, month, day, hour, min, sec
end

local leap_years_since_1970 = leap_years_since(1970)
local function timestamp(year, month, day, hour, min, sec)
	year, month, day, hour, min, sec = normalise(year, month, day, hour, min, sec)

	local days_since_epoch = day_of_year(day, month, year)
			+ 365 * (year - 1970)
			-- Each leap year adds one day
			+ (leap_years_since(year - 1) - leap_years_since_1970) - 1

	return days_since_epoch * (60 * 60 * 24)
			+ hour * (60 * 60)
			+ min * 60
			+ sec
end

local function do_timezone_shift(year, month, day, hour, min, sec, time_zone, ts)
	if time_zone < 9 then
		local nh = hour + time_zone
		if nh < 0 then
			local nd = day - 1
			if nd > 0 then
				hour = 24 + nh
				return year, month, nd, hour, min, sec
			end
		else
			return year, month, day, nh, min, sec
		end
		--logs(ts)
		ts = ts + (time_zone * 3600)
		year, month, day, hour, min, sec = normalise(1970, 1, 1, 0, 0, ts)
		return year, month, day, hour, min, sec, ts
	end
	if time_zone > 20 and time_zone < 60 then
		local nm = min + time_zone
		if nm > 0 then
			return year, month, day, hour, nm, sec
		else
			local nh = hour - 1
			if hour >= 0 then
				min = 60 + nm
				return year, month, day, nh, min, sec
			end
		end
	end
	if time_zone > 60 then
		local nmin = (60 * hour + min) + time_zone
		if nmin > 0 then
			if nmin < 60 then
				return year, month, day, 0, nmin, sec
			else
				hour = floor(nmin / 60)
				min = hour * 60 - nmin
				return year, month, day, hour, min, sec
			end
		end
	end
	ts = ts + (time_zone * 60)
	year, month, day, hour, min, sec = normalise(1970, 1, 1, 0, 0, ts)
	return year, month, day, hour, min, sec, ts
end

---@class datetime
local _M = {
	year = 1970,
	month = 1,
	day = 1,
	hour = 0,
	min = 0,
	sec = 0,
	yday = 0,
	wday = 0,
	time_zone = local_time_zone
}

local function coerce_arg(t)
	if type(t) == 'number' then
		return t
	end
	if t.timestamp then
		return t:timestamp()
	end
	return t
end

local mt = {
	__index = _M,
	__tostring = function(self)
		return self:tostring()
	end,
	__eq = function(a, b)
		return coerce_arg(a) == coerce_arg(b)
	end,
	__lt = function(a, b)
		return coerce_arg(a) < coerce_arg(b)
	end,
	__sub = function(a, b)
		return coerce_arg(a) - coerce_arg(b)
	end,
}

local function new_timetable(year, month, day, hour, min, sec, time_zone)
	local ts
	--if time_zone and time_zone > 0 then
	--	time_zone = _M.get_time_zone(time_zone)
	--	ts = timestamp(year, month, day, hour, min, sec)
	--	year, month, day, hour, min, sec, ts = do_timezone_shift(year, month, day, hour, min, sec, time_zone, ts)
	--end
	local yday = day_of_year(day, month, year)
	local wday = day_of_week(day, month, year)
	local instance = {
		year = year,
		month = month,
		day = day,
		hour = hour,
		min = min,
		sec = sec,
		yday = yday,
		wday = wday,
		ts = ts,
		time_zone = time_zone or 0, -- default utctime
	}
	setmetatable(instance, mt)
	return instance
end

---new
---@param year_or_date_string number|string @ `2018-01-28 00:00:00z` as UTC time, or `2019-01-28 08:00:00` as local  time
---@param month number
---@param day number
---@param hour number
---@param min number
---@param sec number
---@param time_zone number
---@return datetime
function _M.new(year_or_date_string, month, day, hour, min, sec, time_zone)
	time_zone = time_zone or 0
	local year = year_or_date_string
	if type(year_or_date_string) == 'string' then
		local mc = ngx.re.match(year_or_date_string, [[(\d+)-(\d+)-(\d+)[T ](\d+):(\d+):(\d+)([\.\d]+)*(z)*]], 'joi')
		if not mc[8] then
			--NOT end with z like '2016-07-03 22:22:11Z' treat is as local time
			time_zone = local_time_zone
		end
		year, month, day, hour, min, sec = tonumber(mc[1]), tonumber(mc[2]), tonumber(mc[3]), tonumber(mc[4]), tonumber(mc[5]), tonumber(mc[6])
	end
	if time_zone ~= 0 then
		year, month, day, hour, min, sec = normalise(year, month, day, hour - time_zone, min, sec)
	end

	return new_timetable(year, month, day, hour, min, sec, time_zone)
end

---new_by_timestamp
---@param ts number
---@param time_zone number
---@return datetime
function _M.new_by_timestamp(ts)
	local tp = type(ts)
	if tp == "string" then
		error("bad argument #1 to 'new_from_timestamp' (number expected, got " .. tp .. ")", 2)
	end
	local year, month, day, hour, min, sec = normalise(1970, 1, 1, 0, 0, ts)
	return new_timetable(year, month, day, hour, min, sec)
end

---timestamp get current utc timestamp
---@return number
function _M:timestamp()
	local ts = self.ts

	if not ts then
		ts = timestamp(self.year, self.month, self.day, self.hour, self.min, self.sec)
		self.ts = ts
	end
	return ts
end

function _M:rfc_3339()
	local sec, msec = borrow(self.sec, 0, 1000)
	msec = math.floor(msec)
	return strformat("%04u-%02u-%02uT%02u:%02u:%02d.%03d", self.year, self.month, self.day, self.hour, self.min, sec, msec)
end

---tostring standard xxxx-xx-xx xx:xx:xx time
function _M:tostring(time_zone)
	local d, h = self.day, self.hour
	if time_zone and time_zone > 0 then
		local year, month, day, hour, min, sec = do_timezone_shift(self.year, self.month, self.day, self.hour, self.min, self.sec, time_zone, self:timestamp())
		return strformat("%04u-%02u-%02u %02u:%02u:%02d", year, month, day, hour, min, sec)
	else
		return strformat("%04u-%02u-%02u %02u:%02u:%02d", self.year, self.month, d, h, self.min, self.sec)
	end
end

---utc
---@param time_zone number
function _M:utc(time_zone)
	local d, h = self.day, self.hour
	if time_zone and time_zone > 0 then
		local year, month, day, hour, min, sec = do_timezone_shift(self.year, self.month, self.day, self.hour, self.min, self.sec, time_zone, self:timestamp())
		return strformat("%04u-%02u-%02uT%02u:%02u:%02d", year, month, day, hour, min, sec)
	else
		return strformat("%04u-%02u-%02uT%02u:%02u:%02dz", self.year, self.month, d, h, self.min, self.sec)
	end
end

---localtime get date-time string by datet-time object's own timezone
function _M:localtime()
	local tz = self.time_zone
	if tz and tz > 0 then
		return self:utc(tz)
	end
	return self:utc()
end

---localtime get date-time string by local system timezone
function _M:system_time()
	return self:utc(local_time_zone)
end

---clone deep clone a datetime object
---@return datetime
function _M:clone()
	return new_timetable(self.year, self.month, self.day, self.hour, self.min, self.sec)
end

---parse_timestamp
---@param timestamp number @1530098007
---@return number, number, number, number, number, number @year, month, day, hour, min, sec
function _M.parse_timestamp(timestamp, time_zone)
	local year, month, day, hour, min, sec = normalise(1970, 1, 1, 0, 0, timestamp)
	return year, month, day, hour + time_zone, min, sec
end

local timezone = {
	utc = 0, gmt = 0,
	est = 5, edt = 4,
	cst = 6, cdt = 6,
	mst = 7, mdt = 6,
	pst = 8, pdt = 7,
	UTC = 0, GMT = 0,
	EST = 5, EDT = 4,
	CST = 6, CDT = 6,
	MST = 7, MDT = 6,
	PST = 8, PDT = 7,
}

function _M.get_time_zone(time_zone_abbr)
	if time_zone_abbr then
		if type(time_zone_abbr) == 'number' then
			return time_zone_abbr
		end
		return timezone[time_zone_abbr] or 0
	end
	return 0
end

---is_datetime is standard utc string or local string link '2018-01-01T20:12:23Z'
---@param date_str string
---@return boolean,boolean @is datetime string, is utc date string
function _M.is_datetime_string(date_str)
	if not date_str or date_str == '' then
		return nil, 'empty date string'
	end
	local len = #date_str
	if len < 19 or len > 20 then
		return nil, 'bad date format'
	end
	--45=-	46=.	47=/	48=0	49=1	50=2	51=3	52=4	53=5	54=6	55=7	56=8	57=9	58=:
	local num
	for i = 1, 19 do
		num = sbyte(date_str, i)
		if num < 48 then
			if num == 45 and not (i == 5 or i == 8) then
				return false--, 'should be `-`'
			elseif num == 32 and i ~= 11 then
				return false--, 'should be ` `'
			end
		elseif num > 57 then
			if num == 58 and not (i == 14 or i == 17) then
				return false--, 'should be `:`'
			end
			if i == 11 and not (num == 84 or num == 116) then
				return false--, 'should be `T` or `t` or ` `'
			end
		end
	end
	if len == 20 then
		num = sbyte(date_str, 20)
		if not (num == 122 or num == 90) then
			return false--, 'should be `Z` or `z`'
		end
		return true, true
	end
	return true
end

---parse get a datetime object from a string
---@param date_str string @ `2018-06-27T11:13:27z` normal time or utc time
---@param time_zone number|string @ PST|CST|EDT or 8,7,5,4 hours
---@param only_numbers boolean @ true to return Y, M, D, h, m, s numbers
function _M.parse(date_str, time_zone, only_numbers)
	local is_date, is_utc = _M.is_datetime_string(date_str)
	if not is_date then
		return nil, is_utc
	end
	local Y, M, D = tonumber(sub(date_str, 1, 4)), tonumber(sub(date_str, 6, 7)), tonumber(sub(date_str, 9, 10))
	local h, m, s = tonumber(sub(date_str, 12, 13)), tonumber(sub(date_str, 15, 16)), tonumber(sub(date_str, 18, 19))
	if not is_utc then
		time_zone = _M.get_time_zone(time_zone)
		local ts = timestamp(Y, M, D, h, m, s)
		Y, M, D, h, m, s = do_timezone_shift(Y, M, D, h, m, s, time_zone, ts)
	end
	if only_numbers then
		return Y, M, D, h, m, s
	end
	return new_timetable(Y, M, D, h, m, s)
end

function _M:time_shift(year, month, day, hour, min, sec)
	year = self.year + (year or 0)
	month = self.month + (month or 0)
	day = self.day + (day or 0)
	hour = self.hour + (hour or 0)
	min = self.min + (min or 0)
	sec = self.sec + (sec or 0)
	self.year, self.month, self.day, self.hour, self.min, self.sec = normalise(year, month, day, hour, min, sec)
	return self.year, self.month, self.day, self.hour, self.min, self.sec
end

function _M.normalise(year, month, day, hour, min, sec)
	return normalise(year, month, day, hour, min, sec)
end

_M.day_of_year = day_of_year
_M.day_of_week = day_of_week

return _M
