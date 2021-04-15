-- code from  https://github.com/duhoobo/lua-resty-smtp
-- Alternative https://github.com/GUI/lua-resty-mail

local smtp = require("resty.smtp")
local mime = require("resty.smtp.mime")
local ltn12 = require("resty.smtp.ltn12")

---@class mail
local _M = {
	-- You should always change this default_config in first loading
	default_config = {
		--from_name = '管理员',
		--from = 'xxx@xxx.com',
		--title = 'mail title',
		--content = 'mail body here',
		--to = { 'ppp1@ppp.com', 'ppp2@ppp.com', 'ppp3@ppp.com' },
		--attachment_content_type = 'image/png',
		--attachment_file_name = 'test.png',
		--attachment = { 43, 54, 43, 43, 52, 45 }, -- byte or string
		--user = 'xxxx@xxxx.com',
		--password = 'xxxxxxxx',
		--enable_ssl = false,
		--verify_cert = false,
		--server = 'xxx.xxx.xxx.xxx', --ip
		--domain = 'mail.xxxx.com',
		--port = 25 --ssl port 465
	}
}

---send
---@param mail_to string @mail address been sent to
---@param title string
---@param content string @mail content
---@param from string @sender's mail address
---@param from_name string @ sender name displayed in mail header
function _M.send(mail_to, title, content, from, from_name)
	if not mail_to or not title or not content then
		return nil, 'empty parameter not allowed'
	end
	local mail = {
		to = { mail_to },
		title = title,
		content = content,
		from = from,
		from_name = from_name,
	}
	--Because it's sent as async way, no error could captured in this thead!
	_M.send_mail(mail)
end

_M.send_mail = function(mail, mailconf)
	mailconf = mailconf or _M.default_config
	local subject = mail.title
	local body = mail.content
	local from = mail.from or mailconf.from
	local from_name = mail.from_name or mailconf.from_name
	local to = mail.to
	local attachment_content_type = mail.attachment_content_type -- "text/plain" "image/png"
	local attachment_file_name = mail.attachment_file_name -- "README.md" "qrcode.png"
	local attachment = mail.attachment-- "hello world" "byte array"
	local enable_ssl = mail.enable_ssl or mailconf.enable_ssl
	local verify_cert = mail.verify_cert or mailconf.verify_cert
	local user = mail.user or mailconf.user
	local password = mail.password or mailconf.password
	local server = mail.server or mailconf.server
	local domain = mail.domain or mailconf.domain
	local port = mail.port or mailconf.port
	local attachment_obj = nil
	if attachment then
		attachment_obj = {
			headers = {
				["content-type"] = attachment_content_type .. '; name="' .. attachment_file_name .. '"',
				["content-disposition"] = 'attachment; filename="' .. attachment_file_name .. '"',
				["content-transfer-encoding"] = "base64"
			},
			-- attachment encoded here
			body = ltn12.source.chain(
					ltn12.source.string(attachment),
					mime.encode("base64")
			)
		}
	end
	local mesgt = {
		headers = {
			subject = mime.ew(subject, nil, { charset = "utf-8" }),
			from = mime.ew('管理员', nil, { charset = "utf-8" }) .. '<' .. from .. '>',
			to = table.concat(to, ",")
		},
		body = {
			[1] = { body = body },
			[2] = attachment_obj
		}
	}
	-- ngx.log(ngx.ERR, mime.ew(body, nil, {charset = "utf-8"}))
	local mail_obj = {
		from = from,
		rcpt = to,
		user = user,
		password = password,
		server = server,
		port = port,
		domain = domain,
		source = smtp.message(mesgt),
		ssl = { enable = enable_ssl, verify_cert = verify_cert }
	}

	_M.smpt_send(mail_obj)
end

function _M.smpt_send(mail)
	local ret, err = smtp.send(mail)
	if err then
		return nil, 'sent mail fail: ' .. err
	end
end

return _M 