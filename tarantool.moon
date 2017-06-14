mp     = require "MessagePack"
C      = require "const"
string = string
table  = table
ngx    = ngx
type   = type
ipairs = ipairs
error  = error
string = string
socket = nil
decode_base64 = nil
sha1_bin = nil

-- Use non NGINX modules
-- requires: luasock (implicit), lua-resty-socket, sha1

if not ngx then
  socket = require("socket")
  socket.unix = require("socket.unix")
  mime   = require("mime")
  decode_base64 = mime.unb64
  sha1_bin = require("crypto").sha1
else
  socket = ngx.socket
  decode_base64 = ngx.decode_base64
  sha1_bin = ngx.sha1_bin

mp.set_integer('unsigned')

_prepare_request = (h, b) ->
  header = mp.pack(h)
  body   = mp.pack(b)
  len    = mp.pack(string.len(header) + string.len(body))
  len .. header .. body

_xor = (str_a, str_b) ->
  _bxor = (a, b) ->
    r = 0
    for i = 0, 31 do
      x = a / 2 + b / 2
      if x ~= math.floor(x)
          r = r + 2^i
      a = math.floor(a / 2)
      b = math.floor(b / 2)
    return r
  result = ''
  if string.len(str_a) != string.len(str_b) then
    return
  for i = 1, string.len(str_a) do
    result = result .. string.char(_bxor(string.byte(str_a, i), string.byte(str_b, i)))
  result

_prepare_key = (value) ->
  if type(value) == 'table'
    return value
  elseif value == nil
    return { }
  else
    return { value }

class Tarantool
  new: (params) =>
    @meta = {
      host:           C.HOST,
      port:           C.PORT,
      user:           C.USER,
      password:       C.PASSWORD,
      socket_timeout: C.SOCKET_TIMEOUT,
      connect_now:    C.CONNECT_NOW
      _lookup_spaces: true
      _lookup_indexes: true
      _spaces: {}
      _indexes: {}
    }

    if params and type(params) == 'table'
      for key, value in pairs(@meta) do
        if params[key] != nil then
          @meta[key] = params[key]
        self[key] = @meta[key]

    sock, err = socket.tcp()
    if not sock
      @err = err
      return

    if @socket_timeout
      sock\settimeout(@socket_timeout)
    @sock = sock

    if not ngx
      @unix = socket.unix()

    if @connect_now
      ok, err = @connect()
      if not ok
        @err = err

  enable_lookups: () =>
    @_lookup_spaces = true
    @_lookup_indexes = true

  disable_lookups: () =>
    @_lookup_spaces = false
    @_lookup_indexes = false
    @_spaces = {}
    @_indexes = {}

  _wraperr: (err) =>
    if err then
      err .. ', server: ' .. @host .. ':' .. @port
    else
      "Internal error"

  connect: (host, port) =>
    if not @sock
      return nil, "No socket created"

    @host = host or @host
    @port = tonumber(port or @port)

    ok = nil
    err = nil
    if string.find(@host, 'unix:/')
      if ngx
        ok, err = @sock\connect(@host)
      else
        ok, err = @unix\connect(string.match(@host, 'unix:(.+)'))
        if ok
          @sock = @unix
    else
      ok, err = @sock\connect(@host, @port)

    if not ok then
      return ok, @_wraperr(err)
    return @_handshake()

  disconnect: () =>
    if not @sock
      return nil, "no socket created"
    return @sock\close()

  set_keepalive: () =>
    if not @sock
      return nil, "no socket created"
    ok, err = @sock\setkeepalive()
    if not ok then
      @disconnect()
      return nil, err
    return ok

  select: (space, index, key, opts) =>
    if opts == nil
      opts = {}

    spaceno, err = @_resolve_space(space)
    if not spaceno
      return nil, err

    indexno, err = @_resolve_index(spaceno, index)
    if not indexno
      return nil, err

    body = {
      [C.SPACE_ID]: spaceno,
      [C.INDEX_ID]: indexno,
      [C.KEY]:     _prepare_key(key)
    }

    if opts.limit != nil
      body[C.LIMIT] = tonumber(opts.limit)
    else
      body[C.LIMIT] = C.MAX_LIMIT
    if opts.offset != nil then
      body[C.OFFSET] = tonumber(opts.offset)
    else
      body[C.OFFSET] = 0

    if type(opts.iterator) == 'number' then
      body[C.ITERATOR] = opts.iterator

    response, err = @_request({ [ C.TYPE ]: C.SELECT }, body )
    if err
      return nil, err
    elseif response and response.code != C.OK
      return nil, @_wraperr(response.error)
    else
      return response.data

  insert: (space, tuple) =>
    spaceno, err = @_resolve_space(space)
    if not spaceno
      return nil, err

    response, err = @_request({ [C.TYPE]: C.INSERT }, { [C.SPACE_ID]: spaceno, [C.TUPLE]: tuple })
    if err
      return nil, err
    elseif response and response.code != C.OK
      return nil, @_wraperr(response.error)
    else
      return response.data

  replace: (space, tuple) =>
    spaceno, err = @_resolve_space(space)
    if not spaceno
      return nil, err

    response, err = @_request({ [C.TYPE]: C.REPLACE }, { [C.SPACE_ID]: spaceno, [C.TUPLE]: tuple })
    if err
      return nil, err
    elseif response and response.code != C.OK then
      return nil, @_wraperr(response.error)
    else
      return response.data

  delete: (space, key) =>
    spaceno, err = @_resolve_space(space)
    if not spaceno
      return nil, err

    response, err = @_request({ [C.TYPE]: C.DELETE }, { [C.SPACE_ID]: spaceno, [C.KEY]: _prepare_key(key) })
    if err
      return nil, err
    elseif response and response.code != C.OK
      return nil, @_wraperr(response.error)
    else
      return response.data

  update: (index, key, oplist) =>
    spaceno, err = @_resolve_space(space)
    if not spaceno
      return nil, err

    indexno, err = @_resolve_index(spaceno, index)
    if not indexno
      return nil, err

    response, err = @_request({ [C.TYPE]: C.UPDATE }, {
            [C.SPACE_ID]: spaceno,
            [C.INDEX_ID]: indexno,
            [C.KEY]:       _prepare_key(key),
            [C.TUPLE]:    oplist,
        })
    if err
      return nil, err
    elseif response and response.code != C.OK
      return nil, @_wraperr(response.error)
    else
      return response.data

  upsert: (space, tuple, oplist) =>
    spaceno, err = @_resolve_space(space)
    if not spaceno
      return nil, err

    response, err = @_request({ [C.TYPE]: C.UPSERT }, {
            [C.SPACE_ID]: spaceno,
            [C.TUPLE]:    tuple,
            [C.OPS]:      oplist,
        })

    if err
      return nil, err
    elseif response and response.code != C.OK
      return nil, @_wraperr(response.error)
    else
      return response.data

  ping: () =>
    response, err = @_request({ [ C.TYPE ]: C.PING }, {} )
    if err
      return nil, err
    elseif response and response.code != C.OK
      return nil, @_wraperr(response.error)
    else
      return "PONG"

  call: (proc, args) =>
    response, err = @_request({ [ C.TYPE ]: C.CALL }, { [C.FUNCTION_NAME]: proc, [C.TUPLE]: args } )
    if err
      return nil, err
    elseif response and response.code != C.OK
      return nil, @_wraperr(response.error)
    else
      return response.data

  _resolve_space: (space) =>
    if type(space) == 'number' then
      return space
    elseif type(space) == 'string' then
      if @_lookup_spaces and @_spaces[space] then
        return @_spaces[space]
    else
      return nil, 'Invalid space identificator: ' .. space

    data, err = @select(C.SPACE_SPACE, C.INDEX_SPACE_NAME, space)
    if not data or not data[1] or not data[1][1] or err then
      return nil, (err or 'Can\'t find space with identifier: ' .. space)

    newspace = data[1][1]
    if @_lookup_spaces
      @_spaces[space] = newspace
    return newspace

  _resolve_index: (space, index) =>
    if type(index) == 'number' then
      return index
    elseif type(index) == 'string'
      if @lookup_indexes and @_indexes[index]
        return @_indexes[index]
    else
      return nil, 'Invalid index identifier: ' .. index

    spaceno, err = @_resolve_space(space)
    if not spaceno
      return nil, err

    data, err = @select(C.SPACE_INDEX, C.INDEX_INDEX_NAME, { spaceno, index })
    if not data or not data[1] or not data[1][2] or err
      return nil, (err or 'Can\'t find index with identifier: ' .. index)

    newindex = data[1][2]
    if @_lookup_indexes
      @_indexes[index] = newindex
    return newindex

  _handshake: () =>
    greeting = nil
    greeting_err = nil
    if not @_salt
      greeting, greeting_err = @sock\receive(C.GREETING_SIZE)
      if not greeting or greeting_err
        @sock\close()
        return nil, @_wraperr(greeting_err)

      @_salt = string.sub(greeting, C.GREETING_SALT_OFFSET + 1)
      @_salt = string.sub(decode_base64(@_salt), 1, 20)
      @authenticated = @_authenticate()
      return @authenticated
    return true

  _authenticate: () =>
    if not @user then
      return true

    rbody = { [C.USER_NAME]: @user, [C.TUPLE]: { } }

    password = @password or ''
    if password != ''
      step_1   = sha1_bin(@password)
      step_2   = sha1_bin(step_1)
      step_3   = sha1_bin(@_salt .. step_2)
      scramble = _xor(step_1, step_3)
      rbody[C.TUPLE] = { "chap-sha1",  scramble }

    response, err = @_request({ [C.TYPE]: C.AUTH }, rbody)
    if err
      return nil, err
    elseif response and response.code != C.OK
      return nil, @_wraperr(response.error)
    else
      return true

  _request: (header, body) =>
    sock = @sock

    if type(header) != 'table'
      return nil, 'invlid request header'

    @sync_num = ((@sync_num or 0) + 1) % C.REQUEST_PER_CONNECTION
    if not header[C.SYNC]
      header[C.SYNC] = @sync_num
    else
      @sync_num = header[C.SYNC]

    request    = _prepare_request(header, body)
    bytes, err = sock\send(request)

    if bytes == nil then
      sock\close()
      return nil, @_wraperr("Failed to send request: " .. err)

    size, err = sock\receive(C.HEAD_BODY_LEN_SIZE)
    if not size
      sock\close()
      return nil, @_wraperr("Failed to get response size: " .. err)

    size = mp.unpack(size)
    if size
      sock\close()
      return nil, @_wraperr("Client get response invalid size")

    header_and_body, err = sock\receive(size)
    if not header_and_body
      sock\close()
      return nil, @_wraperr("Failed to get response header and body: " .. err)

    iterator = mp.unpacker(header_and_body)
    value, res_header = iterator()
    if type(res_header) != 'table' then
      return nil, @_wraperr("Invalid header: " .. type(res_header) .. " (table expected)")

    if res_header[C.SYNC] != @sync_num then
      return nil, @_wraperr("Invalid header SYNC: request: " .. @sync_num .. " response: " .. res_header[C.SYNC])

    value, res_body = iterator()
    if type(res_body) != 'table'
      res_body = {}

    return { code: res_header[C.TYPE], data: res_body[C.DATA], error: res_body[C.ERROR] }

