extends Node
## REALTIME CLIENT  -  a small, self-contained client for the online service (Supabase Realtime, "Phoenix" protocol v1).
##
## It is NOT an autoload: network_manager.gd creates it as a child (preload) and is the only script that uses it.
## It uses Godot's WebSocketPeer, which also works in the browser build. No tables, no login, no database:
##   * channels  : named rooms that messages are broadcast to (only people who joined the same channel receive them)
##   * presence  : "who is currently in this channel" (used for online dots, room lists and the match search)
## Only the PUBLIC project URL and the PUBLISHABLE key live here. They are meant to be public. The secret / service_role
## key and the database password must NEVER be put in the game.
##
## Safety built in: every incoming frame is size-checked, every channel chan is validated, nothing received is ever
## executed or evaluated, and reconnecting uses a growing delay so a broken connection cannot flood the service.

signal connection_changed(connected: bool)
signal joined(channel: String, ok: bool)
signal presence_changed(channel: String)
signal message(channel: String, event: String, payload: Dictionary)

const PROJECT_URL := "https://umttseichkzenocsinet.supabase.co"
const PUBLISHABLE_KEY := "sb_publishable_qTJSEf3kp538SeDnyeufGA_BgX96ODh"

const HEARTBEAT_SECONDS: float = 25.0
const RECONNECT_MIN: float = 3.0
const RECONNECT_MAX: float = 60.0
const MAX_IN_BYTES: int = 16384
const MAX_OUT_BYTES: int = 2048
const MAX_OUT_PER_SECOND: int = 30    # hard safety cap for this one player, whatever the game code asks for

var connected: bool = false
## Plain counters for the analytics system (read-only for everybody else; they change nothing in the behaviour).
var stats: Dictionary = {"out_msgs": 0, "in_msgs": 0, "out_bytes": 0, "in_bytes": 0, "in_presence": 0,
		"connects": 0, "lat_sum": 0.0, "lat_n": 0, "lat_max": 0.0}
var _sent_at: Dictionary = {}         # ref -> time (ms) of a heartbeat / join, to measure the round trip

var _ws: WebSocketPeer = null
var _want_connection: bool = false
var _ref: int = 0
var _key: String = ""                 # this player's presence key (unique per browser tab)
var _channels: Dictionary = {}        # short chan -> {want, joined, join_ref, presence, meta, state, rejoin_at}
var _hb_left: float = HEARTBEAT_SECONDS
var _fails_in_a_row: int = 0         # connection attempts that never reached the open state
var _retry_left: float = 0.0
var _retry_delay: float = RECONNECT_MIN
var _out_window_start: int = 0
var _out_count: int = 0
var _open_ms: int = 0
## True when the website that hosts the game (e.g. Playgama) forbids connections to the online server.
var policy_blocked: bool = false


func _ready() -> void:
	process_mode = Node.PROCESS_MODE_ALWAYS
	var rng := RandomNumberGenerator.new()
	rng.randomize()
	var bytes := PackedByteArray()
	for i in 9:
		bytes.append(rng.randi() & 0xFF)
	_key = Marshalls.raw_to_base64(bytes).replace("+", "a").replace("/", "b").replace("=", "")


## Every step of the online connection is written to the console and to the on-screen problem report (?debug=1).
## Before this the game stayed silent, so nobody could see WHY online play (custom match, friends, matchmaking) failed.
func _log(line: String) -> void:
	print("[Realtime] ", line)
	if OS.has_feature("web"):
		JavaScriptBridge.eval("window.__ifrLog && window.__ifrLog(%s)" % JSON.stringify(("[Realtime] " + line).left(240)), true)


func is_configured() -> bool:
	return PROJECT_URL != "" and PUBLISHABLE_KEY != ""


## True when the page's security policy (game portals publish one) lets the game connect to Supabase.
func is_allowed_here() -> bool:
	if not OS.has_feature("web"):
		return true
	var r = JavaScriptBridge.eval("(window.__ifrCspReady===true)?window.__ifrCanConnect('%s/'):1" % PROJECT_URL.replace("https://", "wss://"), true)
	return r == null or int(r) != 0


func start() -> void:
	if _want_connection or not is_configured():
		return
	# The portal-policy check is only a hint. It used to switch online play off for the whole visit when it guessed
	# "blocked"; now the game always tries, and writes the real result to the log.
	if not is_allowed_here():
		# Confirmed on Playgama: its Content-Security-Policy lists the only addresses a game may connect to and the
		# Supabase server is not one of them. A connection attempt can only fail with red console errors, so none is made.
		policy_blocked = true
		_log("online server blocked by this website's security policy - multiplayer unavailable here")
		return
	policy_blocked = false
	_fails_in_a_row = 0
	_want_connection = true
	_retry_left = 0.0
	_open()
	set_process(true)


func stop() -> void:
	_want_connection = false
	if _ws != null:
		_ws.close()
	_ws = null
	_mark_closed()


func _open() -> void:
	_open_ms = Time.get_ticks_msec()
	_log("connecting (attempt %d)" % (_fails_in_a_row + 1))
	_ws = WebSocketPeer.new()
	_ws.inbound_buffer_size = 65536
	_ws.outbound_buffer_size = 65536
	_ws.max_queued_packets = 256
	var host := PROJECT_URL.replace("https://", "wss://")
	var url := "%s/realtime/v1/websocket?apikey=%s&vsn=1.0.0" % [host, PUBLISHABLE_KEY.uri_encode()]
	if _ws.connect_to_url(url) != OK:
		_ws = null
		_schedule_retry()


func _schedule_retry() -> void:
	# A network that blocks the address outright (some game portals only allow their own addresses) never
	# answers: after a few tries slow down a lot, and after a few more stop for this visit. The game then
	# simply plays offline instead of filling the browser console and wasting battery.
	_fails_in_a_row += 1
	# Never give up for the whole visit (one bad moment on a phone network used to switch online play off until the
	# page was reloaded): retry quickly at first, then every 20 s.
	_retry_left = minf(RECONNECT_MIN * float(_fails_in_a_row), 20.0)
	_log("connection failed, next try in %d s" % int(_retry_left))


func _mark_closed() -> void:
	var was := connected
	connected = false
	for chan in _channels:
		var ch: Dictionary = _channels[chan]
		ch["joined"] = false
		ch["state"] = {}
	if was:
		connection_changed.emit(false)


func _process(delta: float) -> void:
	if _ws == null:
		if _want_connection:
			_retry_left -= delta
			if _retry_left <= 0.0:
				_open()
		return
	_ws.poll()
	if _ws.get_ready_state() == WebSocketPeer.STATE_CONNECTING and Time.get_ticks_msec() - _open_ms > 12000:
		_log("no answer after 12 s (blocked by the page policy, a firewall or a paused Supabase project?)")
		_ws.close()
		_ws = null
		_mark_closed()
		_schedule_retry()
		return
	match _ws.get_ready_state():
		WebSocketPeer.STATE_OPEN:
			if not connected:
				_log("connected")
				connected = true
				_fails_in_a_row = 0
				stats["connects"] = int(stats["connects"]) + 1
				_retry_delay = RECONNECT_MIN
				_hb_left = HEARTBEAT_SECONDS
				connection_changed.emit(true)
				for chan in _channels.keys():
					_send_join(str(chan))
			while _ws != null and _ws.get_available_packet_count() > 0:
				var packet := _ws.get_packet()
				if packet.size() > MAX_IN_BYTES:
					continue
				stats["in_msgs"] = int(stats["in_msgs"]) + 1
				stats["in_bytes"] = int(stats["in_bytes"]) + packet.size()
				_handle(packet.get_string_from_utf8())
			_hb_left -= delta
			if _hb_left <= 0.0:
				_hb_left = HEARTBEAT_SECONDS
				var hb_ref := _next_ref()
				_sent_at[hb_ref] = Time.get_ticks_msec()
				_push({"topic": "phoenix", "event": "heartbeat", "payload": {}, "ref": hb_ref}, true)
			_retry_pending_joins()
		WebSocketPeer.STATE_CLOSED:
			_log("closed (code %d %s)" % [_ws.get_close_code(), _ws.get_close_reason()])
			_ws = null
			_mark_closed()
			if _want_connection:
				_schedule_retry()


func _next_ref() -> String:
	_ref += 1
	return str(_ref)


func _push(msg: Dictionary, force: bool = false) -> bool:
	if _ws == null or _ws.get_ready_state() != WebSocketPeer.STATE_OPEN:
		return false
	var now := Time.get_ticks_msec()
	if now - _out_window_start >= 1000:
		_out_window_start = now
		_out_count = 0
	if not force and _out_count >= MAX_OUT_PER_SECOND:
		return false
	var text := JSON.stringify(msg)
	if text.length() > MAX_OUT_BYTES and not force:
		return false
	_out_count += 1
	stats["out_msgs"] = int(stats["out_msgs"]) + 1
	stats["out_bytes"] = int(stats["out_bytes"]) + text.length()
	return _ws.send_text(text) == OK


# ================================================================ channels
static func valid_name(chan: String) -> bool:
	if chan.length() < 2 or chan.length() > 48:
		return false
	for i in chan.length():
		var c := chan.unicode_at(i)
		var ok := (c >= 48 and c <= 57) or (c >= 65 and c <= 90) or (c >= 97 and c <= 122) or c == 45 or c == 95 or c == 58
		if not ok:
			return false
	return true


## Joins a channel (or does nothing when it is already wanted). presence = true -> the member list is delivered.
func join(chan: String, presence: bool = false, meta: Dictionary = {}) -> void:
	if not valid_name(chan):
		return
	var ch: Dictionary = _channels.get(chan, {})
	if ch.is_empty():
		ch = {"want": true, "joined": false, "join_ref": "", "presence": presence, "meta": meta.duplicate(true),
				"state": {}, "rejoin_at": 0}
		_channels[chan] = ch
	else:
		ch["want"] = true
		if presence:
			ch["presence"] = true
		if not meta.is_empty():
			ch["meta"] = meta.duplicate(true)
		return
	if connected:
		_send_join(chan)


func _send_join(chan: String) -> void:
	var ch: Dictionary = _channels.get(chan, {})
	if ch.is_empty():
		return
	var ref := _next_ref()
	_sent_at[ref] = Time.get_ticks_msec()
	ch["join_ref"] = ref
	ch["joined"] = false
	ch["rejoin_at"] = 0
	var cfg := {"broadcast": {"ack": false, "self": false},
			"presence": {"key": _key, "enabled": bool(ch["presence"])}, "private": false}
	_push({"topic": "realtime:" + chan, "event": "phx_join", "payload": {"config": cfg},
			"ref": ref, "join_ref": ref}, true)


func leave(chan: String) -> void:
	if not _channels.has(chan):
		return
	_channels.erase(chan)
	if connected:
		_push({"topic": "realtime:" + chan, "event": "phx_leave", "payload": {}, "ref": _next_ref()}, true)


func has_channel(chan: String) -> bool:
	return _channels.has(chan)


func is_joined(chan: String) -> bool:
	return _channels.has(chan) and bool(_channels[chan]["joined"])


## Announces (or updates) this player's own data in the channel's member list.
func track(chan: String, meta: Dictionary) -> void:
	if not _channels.has(chan):
		return
	var ch: Dictionary = _channels[chan]
	ch["meta"] = meta.duplicate(true)
	if bool(ch["joined"]) and bool(ch["presence"]):
		_push({"topic": "realtime:" + chan, "event": "presence", "ref": _next_ref(),
				"payload": {"type": "presence", "event": "track", "payload": meta}}, true)


## Sends one event to everybody else in the channel. Returns false when it could not be sent.
func send(chan: String, event: String, payload: Dictionary) -> bool:
	if not is_joined(chan):
		return false
	return _push({"topic": "realtime:" + chan, "event": "broadcast", "ref": _next_ref(),
			"payload": {"type": "broadcast", "event": event, "payload": payload}})


## Everybody currently in the channel (including this player): an Array of meta dictionaries, each with "_key".
func members(chan: String) -> Array:
	var out: Array = []
	if not _channels.has(chan):
		return out
	var state: Dictionary = _channels[chan]["state"]
	for key in state:
		var metas: Array = state[key]
		if metas.is_empty():
			continue
		var m: Dictionary = (metas[metas.size() - 1] as Dictionary).duplicate(true)
		m["_key"] = str(key)
		out.append(m)
	return out


func my_key() -> String:
	return _key


func _note_latency(ref: String) -> void:
	if not _sent_at.has(ref):
		return
	var ms := float(Time.get_ticks_msec() - int(_sent_at[ref]))
	_sent_at.erase(ref)
	stats["lat_sum"] = float(stats["lat_sum"]) + ms
	stats["lat_n"] = int(stats["lat_n"]) + 1
	stats["lat_max"] = maxf(float(stats["lat_max"]), ms)
	if _sent_at.size() > 64:
		_sent_at.clear()


func _retry_pending_joins() -> void:
	var now := Time.get_ticks_msec()
	for chan in _channels:
		var ch: Dictionary = _channels[chan]
		if bool(ch["want"]) and not bool(ch["joined"]) and int(ch["rejoin_at"]) > 0 and now >= int(ch["rejoin_at"]):
			_send_join(str(chan))


# ================================================================ incoming
func _handle(text: String) -> void:
	var parsed = JSON.parse_string(text)
	if not (parsed is Dictionary):
		return
	var msg: Dictionary = parsed
	var topic := str(msg.get("topic", ""))
	var event := str(msg.get("event", ""))
	if event == "phx_reply":
		_note_latency(str(msg.get("ref", "")))
	elif event == "presence_state" or event == "presence_diff":
		stats["in_presence"] = int(stats["in_presence"]) + 1
	if not topic.begins_with("realtime:"):
		return
	var chan := topic.substr(9)
	if not _channels.has(chan):
		return
	var ch: Dictionary = _channels[chan]
	var payload = msg.get("payload", {})
	if not (payload is Dictionary):
		payload = {}
	match event:
		"phx_reply":
			if str(msg.get("ref", "")) != str(ch["join_ref"]):
				return
			if str(payload.get("status", "")) == "ok":
				ch["joined"] = true
				if bool(ch["presence"]) and not (ch["meta"] as Dictionary).is_empty():
					track(chan, ch["meta"])
				joined.emit(chan, true)
			else:
				ch["joined"] = false
				ch["rejoin_at"] = Time.get_ticks_msec() + 5000
				joined.emit(chan, false)
		"presence_state":
			var fresh: Dictionary = {}
			for key in payload:
				var entry = payload[key]
				if entry is Dictionary and (entry as Dictionary).get("metas") is Array:
					fresh[str(key)] = (entry["metas"] as Array).duplicate(true)
			ch["state"] = fresh
			presence_changed.emit(chan)
		"presence_diff":
			var state: Dictionary = ch["state"]
			var leaves = payload.get("leaves", {})
			if leaves is Dictionary:
				for key in leaves:
					var gone = leaves[key]
					if not state.has(str(key)) or not (gone is Dictionary):
						continue
					var gone_refs: Array = []
					for m in (gone as Dictionary).get("metas", []):
						if m is Dictionary:
							gone_refs.append(str((m as Dictionary).get("phx_ref", "")))
					var keep: Array = []
					for m in state[str(key)]:
						if not gone_refs.has(str((m as Dictionary).get("phx_ref", ""))):
							keep.append(m)
					if keep.is_empty():
						state.erase(str(key))
					else:
						state[str(key)] = keep
			var joins = payload.get("joins", {})
			if joins is Dictionary:
				for key in joins:
					var added = joins[key]
					if not (added is Dictionary):
						continue
					var list: Array = state.get(str(key), [])
					for m in (added as Dictionary).get("metas", []):
						if m is Dictionary:
							list.append(m)
					while list.size() > 4:
						list.pop_front()
					state[str(key)] = list
			ch["state"] = state
			presence_changed.emit(chan)
		"broadcast":
			var inner = payload.get("payload", {})
			if inner is Dictionary:
				message.emit(chan, str(payload.get("event", "")), inner)
		"phx_error", "phx_close":
			if bool(ch["joined"]):
				ch["joined"] = false
				ch["state"] = {}
				joined.emit(chan, false)
			if bool(ch["want"]):
				ch["rejoin_at"] = Time.get_ticks_msec() + 4000
