extends Node
## NETWORK MANAGER  (Autoload, name it exactly:  NetworkManager)
##
## All online features of the game, built on the Supabase Realtime service through realtime_client.gd.
## There is NO game computer and NO database behind it: the clients talk to each other through small named channels.
## Permanent data (saves, purchases) lives in Firebase (account.gd). Nothing in here is ever stored online.
##
## CHANNELS (all names are short and only exist while somebody is in them)
##   i:<inbox>     a player's PRIVATE inbox. The inbox id is a long random secret that is shared only with friends,
##                 so only friends can send direct messages / invitations to it. The owner is "present" in it (online dot).
##   d:<CODE>      the "doorbell" of a friend code: the only place where a stranger can send a friend REQUEST.
##   r:<GAMEID>    one custom match (host = the player who created it). Lobby list, settings, countdown.
##   q:<mode>:<map> the random-match search. Everybody searching the same mode + map sees the same member list.
##   m:<map>:<n>   a free-roam Meetup (at most MEETUP_MAX_PLAYERS per channel, then the next number is used).
##   ifr-active    "active players" list; joined only while the Friends screen is open.
##
## RULES that protect players and the free plan
##   * chat: at most CHAT_MAX_LENGTH (100) characters, one message every CHAT_COOLDOWN (5) seconds, enforced when sending
##     AND again on every received message. Control / invisible characters are removed.
##   * chat lines live only in this device's RAM; every line that is not the newest of its conversation is deleted
##     after CHAT_KEEP_SECONDS (5 minutes).
##   * everything received is treated as untrusted text: sizes, types and numbers are checked, senders are rate limited,
##     friend messages are accepted only from codes in the friend list, a "friend added" answer only after we asked.
##   * nothing of value (coins, wins, purchases) is ever sent through a channel. Wins reach the Playgama leaderboard
##     through the SDK, saves reach Firebase under the Firestore rules.
##   * channels are left as soon as they are not needed (friend channels after a few seconds, rooms after the match starts).
##
## OFFLINE: when the online service cannot be reached, random matches still work (20 s search, then computer drivers).

signal server_status(online: bool)
signal room_info(info: Dictionary)
signal room_error(message: String)
signal room_left
signal queue_status(seconds_left: int, waiting: int)
signal match_found(info: Dictionary)
signal leaderboard_received(entries: Array)
signal friends_received(friends: Array)
signal players_received(players: Array)
signal friend_added(friend_name: String)
signal friend_request_received(from_name: String, code: String)
signal meetup_joined(map_name: String, players: Array)
signal meetup_player_joined(peer_id: int, info: Dictionary)
signal meetup_player_left(peer_id: int)
signal meetup_state_received(peer_id: int, position: Vector3, rotation: Vector3)
signal meetup_chat_received(peer_id: int, player_name: String, message: String)
signal invite_received(from_name: String, room_id: String)
signal chat_received(from_code: String, from_name: String, text: String)
signal chat_log_changed(friend_code: String)
signal room_countdown(seconds: int)
signal notice(message: String)
signal race_message(event: String, payload: Dictionary)

## The live race channel of the running match (see race_sync.gd).
var _race_ch: String = ""
var _browser_online: bool = true
var _was_online_once: bool = false

const RtClient = preload("res://realtime_client.gd")

const MAX_PLAYERS: int = 8
const RANDOM_COUNTDOWN: int = 20      # seconds the search waits for real players before computer drivers fill the rest
const CUSTOM_MIN: int = 2
const MODES: PackedStringArray = ["race", "fight", "flag"]
const ID_CHARS := "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
const MAX_FRIENDS: int = 50
const START_COUNTDOWN: int = 3        # seconds between START and the match loading (custom matches)
const CHAT_MAX_LENGTH: int = 100
const CHAT_COOLDOWN_MS: int = 5000
const CHAT_KEEP_MS: int = 300000      # 5 minutes
const CHAT_RECEIVE_GAP_MS: int = 4000 # a sender faster than this is dropped (a little below the 5 s rule, for lag)
const MEETUP_MAX_PLAYERS: int = 10
const MEETUP_SHARDS: int = 4
const FRIEND_PROBE_GAP_MS: int = 20000
const ACTIVE_CHANNEL := "ifr-active"
const PENDING_TIMEOUT_MS: int = 9000

## Optional: the public address of the game page, e.g. its Playgama URL. When empty, "share" sends the friend code only.
@export var invite_base_url: String = ""

var online: bool = false
var my_code: String = ""
var my_peer: int = 0                  # random number for this play session (identifies the car in a Meetup)
var current_room: Dictionary = {}
## Friend chat lives ONLY here, in this device's RAM: friend code -> [{"mine": bool, "text": String, "t": int}, ...].
var chat_log: Dictionary = {}
var in_meetup: bool = false
var meetup_map: String = ""
var friends: Array = []               # [{code, name, inbox}] (saved with the progress, so it follows the account)

var _rt = null   # the realtime client (realtime_client.gd)
var _my_inbox: String = ""
var _started: bool = false
var _online_flags: Dictionary = {}    # friend code -> bool
var _sent_requests: Dictionary = {}   # friend code -> expiry (ms) of a request WE sent
var _pending_requests: Dictionary = {}# code -> name of requests we received and have not answered
var _last_in: Dictionary = {}         # rate-limit memory: key -> last time (ms)
var _outbox: Dictionary = {}          # channel -> [[event, payload], ...] waiting for the join to finish
var _linger: Dictionary = {}          # channel -> time (ms) after which the channel is left again
var _last_probe_ms: int = -1000000
var _chat_next_ms: int = 0
var _purge_timer: Timer
var _tick_timer: Timer
var _url_code_checked: bool = false

# one operation that waits for a channel join: {"kind": "create"|"join"|"meetup", "ch": String, ...}
var _pend: Dictionary = {}

# custom room
var _room_ch: String = ""
var _room_id: String = ""
var _room_host: bool = false
var _room_ready: bool = false
var _room_cfg: Dictionary = {"mode": "race", "map": "simple", "laps": 1}
var _room_host_code: String = ""
var _room_host_gone_ms: int = 0
var _room_info_pending: bool = false
var _cd_left: float = -1.0
var _cd_shown: int = 0
var _cd_checked: bool = false
var _cd_payload: Dictionary = {}

# random search
var _queued: bool = false
var _queue_ch: String = ""
var _queue_mode: String = ""
var _queue_map: String = "simple"
var _queue_left: int = 0
var _offline_left: int = -1
var _offline_mode: String = ""

# meetup
var _meetup_ch: String = ""
var _meetup_map_wanted: String = "simple"
var _meetup_shard: int = 1
var _meetup_players: Dictionary = {}  # peer -> name
var _meetup_last_sent: Vector3 = Vector3(1e9, 1e9, 1e9)
var _meetup_last_rot: Vector3 = Vector3.ZERO
var _meetup_last_sent_ms: int = 0

# leaderboard cache
var _lb_fetch_ms: int = -1000000


func _ready() -> void:
	process_mode = Node.PROCESS_MODE_ALWAYS
	my_peer = randi_range(1, 1073741823)
	_load_identity()
	_rt = RtClient.new()
	_rt.name = "RealtimeClient"
	add_child(_rt)
	_rt.connection_changed.connect(_on_connection)
	_rt.joined.connect(_on_joined)
	_rt.presence_changed.connect(_on_presence)
	_rt.message.connect(_on_message)
	_tick_timer = Timer.new()
	_tick_timer.wait_time = 1.0
	_tick_timer.autostart = true
	_tick_timer.timeout.connect(_tick)
	add_child(_tick_timer)
	_purge_timer = Timer.new()
	_purge_timer.wait_time = 20.0
	_purge_timer.autostart = true
	_purge_timer.timeout.connect(_purge_chat)
	add_child(_purge_timer)
	SaveSystem.loaded.connect(_on_save_loaded)
	set_process(false)


# ================================================================ identity (friend code, private inbox, friend list)
static func _valid_code(code: String) -> bool:
	if code.length() != 5:
		return false
	for i in code.length():
		if ID_CHARS.find(code[i]) < 0:
			return false
	return true


static func _valid_room_id(id: String) -> bool:
	if id.length() != 6:
		return false
	for i in id.length():
		if ID_CHARS.find(id[i]) < 0:
			return false
	return true


static func _random_id(length: int) -> String:
	var out := ""
	var rng := RandomNumberGenerator.new()
	rng.randomize()
	for i in length:
		out += ID_CHARS[rng.randi() % ID_CHARS.length()]
	return out


func _load_identity() -> void:
	var net: Dictionary = SaveSystem.get_section("network")
	var code := str(net.get("code", "")).to_upper()
	var inbox := str(net.get("inbox", ""))
	var fresh := false
	if not _valid_code(code):
		code = _random_id(5)
		fresh = true
	if inbox.length() < 16 or inbox.length() > 24:
		inbox = _random_id(16)
		fresh = true
	var list: Array = []
	var raw = net.get("friends", [])
	if raw is Array:
		for e in raw:
			if e is Dictionary and _valid_code(str(e.get("code", ""))) and str(e.get("inbox", "")).length() >= 16:
				list.append({"code": str(e["code"]), "name": clean_name(str(e.get("name", "Player"))),
						"inbox": str(e["inbox"]).left(24)})
	my_code = code
	_my_inbox = inbox
	friends = list.slice(0, MAX_FRIENDS)
	if fresh:
		_save_net()


func _save_net() -> void:
	SaveSystem.set_section("network", {"code": my_code, "inbox": _my_inbox, "friends": friends})


func _on_save_loaded() -> void:
	# the saved progress changed (cloud merge): maybe another friend list / code arrived
	var old_code := my_code
	var old_inbox := _my_inbox
	_load_identity()
	if _started and (old_code != my_code or old_inbox != _my_inbox):
		if old_inbox != "":
			_rt.leave("i:" + old_inbox)
		if old_code != "":
			_rt.leave("d:" + old_code)
		online = false
		_join_own()
	friends_received.emit(_friend_entries())


func my_name() -> String:
	return clean_name(GameState.player_name)


# ================================================================ text safety + chat rules
static func clean_text(raw: String) -> String:
	var out := ""
	var last_space := true
	for i in raw.length():
		var c := raw.unicode_at(i)
		if c == 9 or c == 10 or c == 13 or c == 32 or c == 0xA0:
			if not last_space:
				out += " "
				last_space = true
			continue
		if c < 32 or c == 127 or (c >= 0x80 and c < 0xA0) or (c >= 0x200B and c <= 0x200F) \
				or (c >= 0x2028 and c <= 0x202E) or (c >= 0x2060 and c <= 0x206F) or c == 0xFEFF \
				or (c >= 0xD800 and c <= 0xDFFF):
			continue
		out += raw[i]
		last_space = false
	return out.strip_edges().left(CHAT_MAX_LENGTH)


static func clean_name(raw: String) -> String:
	var n := clean_text(raw).left(16).strip_edges()
	return n if n != "" else "Player"


## Same as clean_text (a normal function, so other scripts can call it through the autoload without warnings).
func clean_chat(raw: String) -> String:
	return clean_text(raw)


## Seconds until the next chat message may be sent (0 = now).
func chat_cooldown_left() -> float:
	return maxf(float(_chat_next_ms - Time.get_ticks_msec()) / 1000.0, 0.0)


## True when a message may be sent now (and starts the cooldown). Shows the wait time otherwise.
func _chat_allowed() -> bool:
	var now := Time.get_ticks_msec()
	if now < _chat_next_ms:
		GameState.notify("Wait %d s before sending another message" % int(ceil(float(_chat_next_ms - now) / 1000.0)))
		return false
	_chat_next_ms = now + CHAT_COOLDOWN_MS
	return true


## Generic rate limit for received things: true when `key` has not been seen within `gap_ms`.
func _rate_ok(key: String, gap_ms: int) -> bool:
	var now := Time.get_ticks_msec()
	if now - int(_last_in.get(key, -1000000)) < gap_ms:
		return false
	_last_in[key] = now
	if _last_in.size() > 300:
		for k in _last_in.keys():
			if now - int(_last_in[k]) > 60000:
				_last_in.erase(k)
	return true


## RAM only. At most 60 lines per friend.
func log_chat(friend_code: String, mine: bool, text: String) -> void:
	var lines: Array = chat_log.get(friend_code, [])
	lines.append({"mine": mine, "text": text, "t": Time.get_ticks_msec()})
	while lines.size() > 60:
		lines.pop_front()
	chat_log[friend_code] = lines


## Every line older than 5 minutes is deleted, except the newest line of each conversation.
func _purge_chat() -> void:
	var now := Time.get_ticks_msec()
	for code in chat_log.keys():
		var lines: Array = chat_log[code]
		if lines.size() <= 1:
			continue
		var kept: Array = []
		for i in lines.size():
			var line: Dictionary = lines[i]
			var is_last := i == lines.size() - 1
			if is_last or now - int(line.get("t", now)) < CHAT_KEEP_MS:
				kept.append(line)
		if kept.size() != lines.size():
			chat_log[code] = kept
			chat_log_changed.emit(str(code))


# ================================================================ connection
func connect_to_server() -> void:
	if _started:
		return
	_started = true
	if not _rt.is_configured():
		server_status.emit(false)
		return
	_rt.start()
	_join_own()


func _join_own() -> void:
	_rt.join("i:" + _my_inbox, true, {"c": my_code})
	_rt.join("d:" + my_code, false)


func _on_connection(up: bool) -> void:
	if not up and online:
		online = false
		server_status.emit(false)


func _set_online(value: bool) -> void:
	if online == value:
		return
	if value and _was_online_once:
		GameState.notify("BACK ONLINE")
	elif not value and _was_online_once:
		GameState.notify("ONLINE SERVER DISCONNECTED - RECONNECTING...")
	if value:
		_was_online_once = true
	online = value
	server_status.emit(value)
	if value:
		_check_url_code()


func _check_url_code() -> void:
	if _url_code_checked or not OS.has_feature("web"):
		return
	_url_code_checked = true
	# the link can carry the code as ?payload=XXXXX (Playgama hands it over) or ?code=XXXXX (own page)
	var raw = JavaScriptBridge.eval("(new URLSearchParams(window.location.search)).get('code') || ''", true)
	var code := str(raw).strip_edges().to_upper()
	if code == "" and Bridge != null and Bridge.platform != null:
		var payload = Bridge.platform.payload
		if payload != null:
			code = str(payload).strip_edges().to_upper()
	if _valid_code(code) and code != my_code and _friend_index(code) < 0:
		add_friend(code)
	elif _valid_room_id(code):
		join_room(code)   # a room link: open the custom lobby of the friend who sent it


## The browser knows at once when the device loses its internet connection.
func _watch_browser() -> void:
	if not OS.has_feature("web"):
		return
	var on = JavaScriptBridge.eval("navigator.onLine", true)
	var now_on := on == null or bool(on)
	if now_on != _browser_online:
		_browser_online = now_on
		GameState.notify("INTERNET CONNECTED AGAIN" if now_on else "INTERNET DISCONNECTED - CHECK YOUR CONNECTION")


func _tick() -> void:
	_watch_browser()
	var now := Time.get_ticks_msec()
	# channels that were only needed for a moment
	for ch in _linger.keys():
		if now >= int(_linger[ch]):
			_linger.erase(ch)
			_outbox.erase(ch)
			_rt.leave(str(ch))
	# a join that never finished
	if not _pend.is_empty() and now > int(_pend.get("deadline", 0)):
		_pend_fail()
	for code in _sent_requests.keys():
		if now > int(_sent_requests[code]):
			_sent_requests.erase(code)
	_tick_queue()
	_tick_room(now)


# ================================================================ channel events
func _on_joined(ch: String, ok: bool) -> void:
	if ch == "i:" + _my_inbox:
		_set_online(ok)
		return
	if not _pend.is_empty() and str(_pend["ch"]) == ch:
		if ok:
			_pend["joined"] = true
			_pend_try()
		else:
			_pend_fail()
		return
	if ok and _outbox.has(ch):
		for item in _outbox[ch]:
			_rt.send(ch, str(item[0]), item[1])
		_outbox.erase(ch)
	elif not ok:
		_outbox.erase(ch)


func _on_presence(ch: String) -> void:
	if not _pend.is_empty() and str(_pend["ch"]) == ch:
		_pend["state"] = true
		_pend_try()
		return
	if ch.begins_with("i:") and ch != "i:" + _my_inbox:
		_update_online_flag(ch)
	elif ch == ACTIVE_CHANNEL:
		_emit_players()
	elif ch == _room_ch and _room_ready:
		_on_room_presence()
	elif ch == _meetup_ch and in_meetup:
		_on_meetup_presence()


func _on_message(ch: String, event: String, payload: Dictionary) -> void:
	if ch == "i:" + _my_inbox:
		_on_inbox(event, payload)
	elif ch == "d:" + my_code:
		if event == "req":
			_on_friend_request(payload)
	elif ch == _room_ch and _room_ready:
		_on_room_message(event, payload)
	elif ch == _queue_ch and _queued:
		if event == "go":
			_on_queue_go(payload)
	elif ch == _meetup_ch and in_meetup:
		_on_meetup_message(event, payload)
	elif ch == _race_ch and _race_ch != "":
		race_message.emit(event, payload)


## Sends one event to a channel, joining it first when needed; the channel is left again after `linger_ms`.
func _send_oneshot(ch: String, event: String, payload: Dictionary, presence: bool, linger_ms: int) -> void:
	var now := Time.get_ticks_msec()
	_linger[ch] = maxi(int(_linger.get(ch, 0)), now + linger_ms)
	if _rt.is_joined(ch):
		_rt.send(ch, event, payload)
		return
	var box: Array = _outbox.get(ch, [])
	if box.size() < 4:
		box.append([event, payload])
	_outbox[ch] = box
	_rt.join(ch, presence)


# ================================================================ friends
func _friend_index(code: String) -> int:
	for i in friends.size():
		if str(friends[i]["code"]) == code:
			return i
	return -1


func _friend(code: String) -> Dictionary:
	var i := _friend_index(code)
	return friends[i] if i >= 0 else {}


func _friend_entries() -> Array:
	var out: Array = []
	for f in friends:
		out.append({"name": str(f["name"]), "code": str(f["code"]), "online": bool(_online_flags.get(str(f["code"]), false))})
	return out


func _update_online_flag(ch: String) -> void:
	for f in friends:
		if "i:" + str(f["inbox"]) == ch:
			var is_on := false
			for m in _rt.members(ch):
				if str(m.get("c", "")) == str(f["code"]):
					is_on = true
			if bool(_online_flags.get(str(f["code"]), false)) != is_on:
				_online_flags[str(f["code"])] = is_on
				friends_received.emit(_friend_entries())
			return


## Reports the friend list at once and (at most every 20 s) checks who is online.
func request_friends() -> void:
	friends_received.emit(_friend_entries())
	if not online or friends.is_empty():
		return
	var now := Time.get_ticks_msec()
	if now - _last_probe_ms < FRIEND_PROBE_GAP_MS:
		return
	_last_probe_ms = now
	for f in friends:
		var ch := "i:" + str(f["inbox"])
		_linger[ch] = maxi(int(_linger.get(ch, 0)), now + 7000)
		_rt.join(ch, true)


func add_friend(raw_code: String) -> void:
	var code := raw_code.strip_edges().to_upper()
	if not _valid_code(code):
		room_error.emit("That friend code is not valid")
		return
	if code == my_code:
		room_error.emit("That is your own code")
		return
	if _friend_index(code) >= 0:
		room_error.emit("That player is already your friend")
		return
	if friends.size() >= MAX_FRIENDS:
		room_error.emit("Your friend list is full (%d friends)" % MAX_FRIENDS)
		return
	if not online:
		room_error.emit("Not available right now")
		return
	_sent_requests[code] = Time.get_ticks_msec() + 180000
	_send_oneshot("d:" + code, "req", {"c": my_code, "n": my_name(), "i": _my_inbox}, false, 4000)
	notice.emit("Friend request sent. Your friend must be online to accept it.")


func _on_friend_request(p: Dictionary) -> void:
	var code := str(p.get("c", "")).to_upper()
	var inbox := str(p.get("i", ""))
	if not _valid_code(code) or code == my_code or inbox.length() < 16 or inbox.length() > 24:
		return
	if _friend_index(code) >= 0 or friends.size() >= MAX_FRIENDS or _pending_requests.size() >= 5:
		return
	if not _rate_ok("req:" + code, 10000):
		return
	_pending_requests[code] = {"name": clean_name(str(p.get("n", ""))), "inbox": inbox}
	friend_request_received.emit(str(_pending_requests[code]["name"]), code)


func accept_friend(code: String) -> void:
	if not _pending_requests.has(code):
		return
	var req: Dictionary = _pending_requests[code]
	_pending_requests.erase(code)
	if _friend_index(code) < 0 and friends.size() < MAX_FRIENDS:
		friends.append({"code": code, "name": str(req["name"]), "inbox": str(req["inbox"])})
		_save_net()
		Account.sync_soon("friends")
	_send_oneshot("i:" + str(req["inbox"]), "acc", {"c": my_code, "n": my_name(), "i": _my_inbox}, true, 5000)
	friend_added.emit(str(req["name"]))
	friends_received.emit(_friend_entries())


func decline_friend(code: String) -> void:
	_pending_requests.erase(code)


func _on_inbox(event: String, p: Dictionary) -> void:
	var code := str(p.get("f", p.get("c", ""))).to_upper()
	match event:
		"acc":
			# only an answer to a request WE sent
			if not _sent_requests.has(code) or _friend_index(code) >= 0 or friends.size() >= MAX_FRIENDS:
				return
			var inbox := str(p.get("i", ""))
			if inbox.length() < 16 or inbox.length() > 24:
				return
			_sent_requests.erase(code)
			friends.append({"code": code, "name": clean_name(str(p.get("n", ""))), "inbox": inbox})
			_save_net()
			Account.sync_soon("friends")
			friend_added.emit(str(friends[friends.size() - 1]["name"]))
			friends_received.emit(_friend_entries())
		"dm":
			var f := _friend(code)
			if f.is_empty() or not _rate_ok("dm:" + code, CHAT_RECEIVE_GAP_MS):
				return
			var text := clean_text(str(p.get("t", "")))
			if text == "":
				return
			log_chat(code, false, text)
			chat_received.emit(code, str(f["name"]), text)
		"inv":
			var f2 := _friend(code)
			var room := str(p.get("r", "")).to_upper()
			if f2.is_empty() or not _valid_room_id(room) or not _rate_ok("inv:" + code, 5000):
				return
			invite_received.emit(str(f2["name"]), room)


## Friend chat. Returns true when the message was sent (false: empty, cooling down, not a friend, not online).
func send_chat(friend_code: String, text: String) -> bool:
	var f := _friend(friend_code)
	var clean := clean_text(text)
	if f.is_empty() or clean == "":
		return false
	if not online:
		room_error.emit("Not available right now")
		return false
	if not _chat_allowed():
		return false
	log_chat(friend_code, true, clean)
	_send_oneshot("i:" + str(f["inbox"]), "dm", {"f": my_code, "n": my_name(), "t": clean}, true, 120000)
	return true


func invite_friend(friend_code: String) -> void:
	var f := _friend(friend_code)
	if not online or f.is_empty():
		room_error.emit("Not available right now")
		return
	if current_room.is_empty() or not _room_host or _room_id == "":
		room_error.emit("Create a custom match first")
		return
	if (current_room.get("players", []) as Array).size() >= MAX_PLAYERS:
		room_error.emit("The lobby is full")
		return
	_send_oneshot("i:" + str(f["inbox"]), "inv", {"f": my_code, "n": my_name(), "r": _room_id}, true, 6000)
	notice.emit("Invitation sent")


## "Active players": everybody who has the Friends screen open right now (the channel is left again after 90 s).
func request_players() -> void:
	if not online:
		players_received.emit([])
		return
	var now := Time.get_ticks_msec()
	_linger[ACTIVE_CHANNEL] = now + 90000
	var meta := {"c": my_code, "n": my_name()}
	if _rt.is_joined(ACTIVE_CHANNEL):
		_rt.track(ACTIVE_CHANNEL, meta)
		_emit_players()
	else:
		_rt.join(ACTIVE_CHANNEL, true, meta)


func _emit_players() -> void:
	var out: Array = []
	var seen: Dictionary = {}
	for m in _rt.members(ACTIVE_CHANNEL):
		var code := str(m.get("c", "")).to_upper()
		if not _valid_code(code) or code == my_code or seen.has(code):
			continue
		seen[code] = true
		out.append({"name": clean_name(str(m.get("n", ""))), "code": code})
		if out.size() >= 30:
			break
	players_received.emit(out)


# ================================================================ pending join (create / join / meetup)
func _pend_begin(kind: String, ch: String, extra: Dictionary = {}) -> void:
	_pend = {"kind": kind, "ch": ch, "joined": false, "state": false,
			"deadline": Time.get_ticks_msec() + PENDING_TIMEOUT_MS}
	_pend.merge(extra)
	_rt.join(ch, true)


func _pend_try() -> void:
	if _pend.is_empty() or not bool(_pend["joined"]) or not bool(_pend["state"]):
		return
	var p := _pend
	_pend = {}
	match str(p["kind"]):
		"create":
			_resolve_create(p)
		"join":
			_resolve_join(p)
		"meetup":
			_resolve_meetup(p)


func _pend_fail() -> void:
	var kind := str(_pend.get("kind", ""))
	var ch := str(_pend.get("ch", ""))
	_pend = {}
	if ch != "":
		_rt.leave(ch)
	match kind:
		"create":
			_room_reset()
			room_error.emit("Could not create the match. Please try again.")
		"join":
			_room_reset()
			room_error.emit("Could not join that match. Please try again.")
		"meetup":
			_meetup_reset()
			room_error.emit("Could not join the Meetup. Please try again.")


func _leave_all_modes() -> void:
	cancel_match()
	race_leave()
	if not _pend.is_empty():
		_rt.leave(str(_pend["ch"]))
		_pend = {}
	if _room_ch != "":
		if _room_host and _room_ready:
			_rt.send(_room_ch, "close", {})
		_rt.leave(_room_ch)
	_room_reset()
	leave_meetup()


# ================================================================ custom match (private room)
func _room_reset() -> void:
	_room_ch = ""
	_room_id = ""
	_room_host = false
	_room_ready = false
	_room_host_code = ""
	_room_host_gone_ms = 0
	_room_info_pending = false
	_cd_left = -1.0
	_cd_payload = {}
	current_room = {}
	set_process(false)


func create_room(mode: String) -> void:
	if not online:
		room_error.emit("Online play is not available right now. Please try again in a moment.")
		return
	if not MODES.has(mode):
		room_error.emit("Unknown game mode")
		return
	_leave_all_modes()
	_room_cfg = {"mode": mode, "map": "advanced" if GameState.selected_mode == "advanced" else "simple", "laps": 1}
	_create_attempt(0)


func _create_attempt(attempt: int) -> void:
	_room_id = _random_id(6)
	_room_ch = "r:" + _room_id
	_room_host = true
	_room_ready = false
	_pend_begin("create", _room_ch, {"attempt": attempt})


func _resolve_create(p: Dictionary) -> void:
	if not _rt.members(_room_ch).is_empty():
		# somebody already uses this id (extremely unlikely): take another one
		_rt.leave(_room_ch)
		if int(p.get("attempt", 0)) >= 4:
			_room_reset()
			room_error.emit("Could not create the match. Please try again.")
		else:
			_create_attempt(int(p.get("attempt", 0)) + 1)
		return
	_room_host_code = my_code
	_room_ready = true
	_rt.track(_room_ch, {"c": my_code, "n": my_name(), "h": 1, "v": Progress.drivable_car(), "t": Time.get_unix_time_from_system()})
	_schedule_info()


func join_room(game_id: String) -> void:
	if not online:
		room_error.emit("Online play is not available right now. Please try again in a moment.")
		return
	var id := game_id.strip_edges().to_upper()
	if not _valid_room_id(id):
		room_error.emit("A Game ID has 6 characters")
		return
	if id == _room_id and _room_ready:
		room_info.emit(current_room)
		return
	_leave_all_modes()
	_room_id = id
	_room_ch = "r:" + id
	_room_host = false
	_room_ready = false
	_pend_begin("join", _room_ch)


func _resolve_join(_p: Dictionary) -> void:
	var members: Array = _rt.members(_room_ch)
	var host_code := ""
	for m in members:
		if int(m.get("h", 0)) == 1:
			host_code = str(m.get("c", ""))
	if host_code == "":
		_rt.leave(_room_ch)
		_room_reset()
		room_error.emit("Invalid Game ID")
		return
	if members.size() >= MAX_PLAYERS:
		_rt.leave(_room_ch)
		_room_reset()
		room_error.emit("The lobby is full")
		return
	_room_host_code = host_code
	_room_ready = true
	_rt.track(_room_ch, {"c": my_code, "n": my_name(), "v": Progress.drivable_car(), "t": Time.get_unix_time_from_system()})
	current_room = _room_snapshot()
	room_info.emit(current_room)


## Members in lobby order: the host first, then by join time.
func _room_members_sorted() -> Array:
	var list: Array = []
	var seen: Dictionary = {}
	for m in _rt.members(_room_ch):
		var code := str(m.get("c", "")).to_upper()
		if not _valid_code(code) or seen.has(code):
			continue
		seen[code] = true
		list.append({"c": code, "n": clean_name(str(m.get("n", ""))), "h": int(m.get("h", 0)), "v": str(m.get("v", "scorpio")).left(40), "t": float(m.get("t", 0.0))})
	list.sort_custom(func(a, b):
		if int(a["h"]) != int(b["h"]):
			return int(a["h"]) > int(b["h"])
		if float(a["t"]) != float(b["t"]):
			return float(a["t"]) < float(b["t"])
		return str(a["c"]) < str(b["c"]))
	return list


func _room_snapshot(list: Array = []) -> Dictionary:
	if list.is_empty():
		list = _room_members_sorted()
	var names: Array = []
	var host_name := ""
	for e in list:
		names.append(str(e["n"]))
		if str(e["c"]) == _room_host_code:
			host_name = str(e["n"])
	return {"room_id": _room_id, "mode": str(_room_cfg["mode"]), "map": str(_room_cfg["map"]),
			"laps": int(_room_cfg["laps"]), "host": host_name, "host_code": _room_host_code, "players": names,
			"count": names.size(), "max": MAX_PLAYERS, "min": CUSTOM_MIN, "started": false, "countdown": 0}


func _on_room_presence() -> void:
	if _room_host:
		_schedule_info()
		return
	var has_host := false
	for m in _rt.members(_room_ch):
		if int(m.get("h", 0)) == 1:
			has_host = true
	if has_host:
		_room_host_gone_ms = 0
	elif _room_host_gone_ms == 0:
		_room_host_gone_ms = Time.get_ticks_msec()


func _schedule_info() -> void:
	if _room_info_pending:
		return
	_room_info_pending = true
	get_tree().create_timer(0.3).timeout.connect(_flush_info)


## Host only: sends the room state to everybody in the room and shows it locally.
func _flush_info() -> void:
	_room_info_pending = false
	if not _room_host or not _room_ready or _room_ch == "":
		return
	var list: Array = _room_members_sorted()
	while list.size() > MAX_PLAYERS:
		var extra: Dictionary = list.pop_back()
		_rt.send(_room_ch, "kick", {"c": str(extra["c"])})
	current_room = _room_snapshot(list)
	var pairs: Array = []
	for e in list:
		pairs.append([str(e["c"]), str(e["n"])])
	_rt.send(_room_ch, "info", {"hc": my_code, "m": str(_room_cfg["mode"]), "mp": str(_room_cfg["map"]),
			"l": int(_room_cfg["laps"]), "p": pairs})
	room_info.emit(current_room)


func _on_room_message(event: String, p: Dictionary) -> void:
	match event:
		"info":
			if _room_host or str(p.get("hc", "")) != _room_host_code:
				return
			var mode := str(p.get("m", "race"))
			if not MODES.has(mode):
				return
			_room_cfg = {"mode": mode, "map": "advanced" if str(p.get("mp", "")) == "advanced" else "simple",
					"laps": clampi(int(p.get("l", 1)), 1, 5)}
			var list: Array = []
			var raw = p.get("p", [])
			if raw is Array:
				for e in raw:
					if e is Array and e.size() == 2 and _valid_code(str(e[0])) and list.size() < MAX_PLAYERS:
						list.append({"c": str(e[0]), "n": clean_name(str(e[1]))})
			if list.is_empty():
				return
			current_room = _room_snapshot(list)
			room_info.emit(current_room)
		"kick":
			if str(p.get("c", "")) == my_code and not _room_host:
				_room_close("The lobby is full")
		"close":
			if not _room_host:
				_room_close("The host left the room")
		"start":
			if not _room_host:
				_begin_countdown(p)


func _room_close(message: String) -> void:
	if _room_ch != "":
		_rt.leave(_room_ch)
	_room_reset()
	room_error.emit(message)
	room_left.emit()


func _tick_room(now: int) -> void:
	if _room_ready and not _room_host and _room_host_gone_ms > 0 and now - _room_host_gone_ms > 3000:
		_room_close("The host left the room")


## Host of a custom lobby: map ("simple" / "advanced"), mode and laps (1-5, Race only).
func set_room_settings(map_name: String, mode: String, laps: int) -> void:
	if not _room_host or not _room_ready or _cd_left >= 0.0 or not MODES.has(mode):
		return
	_room_cfg = {"mode": mode, "map": "advanced" if map_name == "advanced" else "simple", "laps": clampi(laps, 1, 5)}
	_schedule_info()


func start_room() -> void:
	if not _room_host or not _room_ready or _cd_left >= 0.0:
		return
	var list: Array = _room_members_sorted()
	if list.size() < CUSTOM_MIN:
		room_error.emit("At least %d players are needed" % CUSTOM_MIN)
		return
	var names: Array = []
	var cars: Array = []
	for e in list.slice(0, MAX_PLAYERS):
		names.append(str(e["n"]))
		cars.append(str(e.get("v", "scorpio")))
	var payload := {"s": randi(), "cd": START_COUNTDOWN, "m": str(_room_cfg["mode"]), "mp": str(_room_cfg["map"]),
			"l": int(_room_cfg["laps"]), "n": names, "vk": cars}
	_rt.send(_room_ch, "start", payload)
	_begin_countdown(payload)


func _begin_countdown(p: Dictionary) -> void:
	if _cd_left >= 0.0 or not _room_ready:
		return
	var mode := str(p.get("m", "race"))
	if not MODES.has(mode):
		return
	_cd_payload = p.duplicate(true)
	_cd_payload["m"] = mode
	var seconds := clampi(int(p.get("cd", START_COUNTDOWN)), 1, 5)
	_cd_shown = seconds
	_cd_left = float(seconds)
	_cd_checked = false
	room_countdown.emit(seconds)
	set_process(true)


func _process(delta: float) -> void:
	if _cd_left < 0.0:
		set_process(false)
		return
	_cd_left -= delta
	var shown := int(ceil(_cd_left))
	if shown < _cd_shown and shown > 0:
		_cd_shown = shown
		room_countdown.emit(shown)
	if not _cd_checked and _cd_left <= 1.0:
		# one second before the start: if somebody left, the start is cancelled (checked early, because the
		# first devices to finish leave the room, which must not look like "somebody left" to the others)
		_cd_checked = true
		var members: Array = _room_members_sorted()
		if members.size() < CUSTOM_MIN:
			_cd_left = -1.0
			_cd_payload = {}
			current_room = _room_snapshot(members)
			room_info.emit(current_room)
			return
	if _cd_left <= 0.0:
		_cd_left = -1.0
		_finish_countdown()


func _finish_countdown() -> void:
	var members: Array = _room_members_sorted()
	var names: Array = []
	var raw = _cd_payload.get("n", [])
	if raw is Array:
		for n in raw:
			if names.size() < MAX_PLAYERS:
				names.append(clean_name(str(n)))
	if names.is_empty():
		for e in members:
			names.append(str(e["n"]))
	var info := {"room_id": _room_id, "mode": str(_cd_payload.get("m", "race")),
			"map": "advanced" if str(_cd_payload.get("mp", "")) == "advanced" else "simple",
			"laps": clampi(int(_cd_payload.get("l", 1)), 1, 5), "players": names, "count": names.size(),
			"real_players": names.size(), "seed": int(_cd_payload.get("s", 0)), "started": true,
			"vehicles": _clean_cars(_cd_payload.get("vk", []), names.size())}
	# the room is no longer needed once the match starts
	_rt.leave(_room_ch)
	_room_reset()
	_queued = false
	_begin_match(info, "custom")


func leave_room() -> void:
	if _room_ch != "":
		if _room_host and _room_ready:
			_rt.send(_room_ch, "close", {})
		_rt.leave(_room_ch)
	if not _pend.is_empty() and str(_pend["kind"]) != "meetup":
		_pend = {}
	_room_reset()


# ================================================================ random match
func find_match(mode: String) -> void:
	_queued = true
	if online and MODES.has(mode):
		leave_room()      # nobody searching for a random match can be in a private room or a Meetup
		leave_meetup()
		var map_name := "advanced" if GameState.selected_mode == "advanced" else "simple"
		_queue_mode = mode
		_queue_map = map_name
		_queue_left = RANDOM_COUNTDOWN
		_queue_ch = "q:%s:%s" % [mode, map_name]
		_rt.join(_queue_ch, true, {"c": my_code, "n": my_name(), "v": Progress.drivable_car(), "t": Time.get_unix_time_from_system()})
		queue_status.emit(RANDOM_COUNTDOWN, 1)
	else:
		_offline_mode = mode
		_offline_left = RANDOM_COUNTDOWN - 1
		_offline_tick()


func cancel_match() -> void:
	_queued = false
	_offline_left = -1
	if _queue_ch != "":
		_rt.leave(_queue_ch)
		_queue_ch = ""


func _queue_members_sorted() -> Array:
	var list: Array = []
	var seen: Dictionary = {}
	for m in _rt.members(_queue_ch):
		var code := str(m.get("c", "")).to_upper()
		if not _valid_code(code) or seen.has(code):
			continue
		seen[code] = true
		list.append({"c": code, "n": clean_name(str(m.get("n", ""))), "v": str(m.get("v", "scorpio")).left(40), "t": float(m.get("t", 0.0))})
	list.sort_custom(func(a, b):
		if float(a["t"]) != float(b["t"]):
			return float(a["t"]) < float(b["t"])
		return str(a["c"]) < str(b["c"]))
	return list


func _tick_queue() -> void:
	if not _queued or _queue_ch == "":
		return
	_queue_left -= 1
	var members: Array = _queue_members_sorted()
	var waiting := clampi(members.size(), 1, MAX_PLAYERS)
	queue_status.emit(maxi(_queue_left, 0), waiting)
	if not _rt.is_joined(_queue_ch) or members.is_empty():
		if _queue_left <= 0:
			_queue_launch_alone()
		return
	var leader := str(members[0]["c"])
	if leader == my_code:
		# a full room starts at once; otherwise the search ends after RANDOM_COUNTDOWN seconds
		if _queue_left <= 0 or members.size() >= MAX_PLAYERS:
			_queue_send_go(members)
	elif _queue_left <= -3:
		_queue_send_go(members)   # the leader stayed silent: take over


func _queue_send_go(members: Array) -> void:
	var codes: Array = []
	var names: Array = []
	var cars: Array = []
	for e in members.slice(0, MAX_PLAYERS):
		codes.append(str(e["c"]))
		names.append(str(e["n"]))
		cars.append(str(e.get("v", "scorpio")))
	var payload := {"m": codes, "n": names, "vk": cars, "s": randi()}
	_rt.send(_queue_ch, "go", payload)
	_queue_start(payload)


func _on_queue_go(p: Dictionary) -> void:
	var codes = p.get("m", [])
	if not (codes is Array) or (codes as Array).size() > MAX_PLAYERS or not (codes as Array).has(my_code):
		return
	_queue_start(p)


func _queue_start(p: Dictionary) -> void:
	var names: Array = []
	var raw = p.get("n", [])
	if raw is Array:
		for n in raw:
			if names.size() < MAX_PLAYERS:
				names.append(clean_name(str(n)))
	if names.is_empty():
		names = [my_name()]
	var cars: Array = _clean_cars(p.get("vk", []), names.size())
	var mode := _queue_mode
	var map_name := _queue_map
	cancel_match()
	_begin_match({"room_id": "RANDOM", "mode": mode, "map": map_name, "players": names, "vehicles": cars, "count": names.size(),
			"real_players": names.size(), "seed": int(p.get("s", 0)), "started": true}, "random")


## The vehicle model of every real player, in the same order as the player names.
func _clean_cars(raw, count: int) -> Array:
	var out: Array = []
	if raw is Array:
		for c in raw:
			if out.size() < count:
				out.append(str(c).left(40))
	return out


func _queue_launch_alone() -> void:
	var mode := _queue_mode
	cancel_match()
	_begin_match({"room_id": "OFFLINE", "mode": mode, "players": [my_name()], "count": 1, "real_players": 1,
			"seed": randi()}, "random")


func _offline_tick() -> void:
	if _offline_left < 0 or not _queued:
		return
	queue_status.emit(_offline_left, 1)
	if _offline_left == 0:
		_offline_left = -1
		_queued = false
		_begin_match({"room_id": "OFFLINE", "mode": _offline_mode, "players": [GameState.player_name],
				"count": 1, "real_players": 1, "seed": randi()}, "random")
		return
	_offline_left -= 1
	get_tree().create_timer(1.0).timeout.connect(_offline_tick)


# ================================================================ meetup (free roam)
func _meetup_reset() -> void:
	_meetup_ch = ""
	_meetup_players = {}
	in_meetup = false
	meetup_map = ""


func join_meetup(requested_map: String) -> bool:
	if not online:
		return false
	_leave_all_modes()
	_meetup_map_wanted = "advanced" if requested_map == "advanced" else "simple"
	_meetup_shard = 1
	_meetup_try_shard()
	return true


func _meetup_try_shard() -> void:
	_meetup_ch = "m:%s:%d" % [_meetup_map_wanted, _meetup_shard]
	_pend_begin("meetup", _meetup_ch)


func _resolve_meetup(_p: Dictionary) -> void:
	var members: Array = _rt.members(_meetup_ch)
	if members.size() >= MEETUP_MAX_PLAYERS:
		_rt.leave(_meetup_ch)
		_meetup_shard += 1
		if _meetup_shard > MEETUP_SHARDS:
			_meetup_reset()
			room_error.emit("The Meetup is full (%d players). Please try again in a moment" % MEETUP_MAX_PLAYERS)
		else:
			_meetup_try_shard()
		return
	var vehicle := Progress.drivable_car()
	_rt.track(_meetup_ch, {"p": my_peer, "n": my_name(), "v": vehicle})
	_meetup_players = {}
	var initial: Array = []
	for m in members:
		var peer := int(m.get("p", 0))
		if peer > 0 and peer != my_peer and not _meetup_players.has(peer):
			_meetup_players[peer] = clean_name(str(m.get("n", "")))
			initial.append(_meetup_info(peer, m))
	in_meetup = true
	meetup_map = _meetup_map_wanted
	_meetup_last_sent = Vector3(1e9, 1e9, 1e9)
	GameState.selected_mode = meetup_map
	GameState.game_mode = "meetup"
	GameState.match_type = "meetup"
	GameState.match_info = {"meetup_players": initial.duplicate(true)}
	meetup_joined.emit(meetup_map, initial)


func _meetup_info(peer: int, m: Dictionary) -> Dictionary:
	return {"peer_id": peer, "name": clean_name(str(m.get("n", ""))), "vehicle": str(m.get("v", "scorpio")).left(40),
			"position": Vector3.ZERO, "rotation": Vector3.ZERO}


func _on_meetup_presence() -> void:
	var now_peers: Dictionary = {}
	for m in _rt.members(_meetup_ch):
		var peer := int(m.get("p", 0))
		if peer <= 0 or peer == my_peer:
			continue
		now_peers[peer] = true
		if not _meetup_players.has(peer):
			if _meetup_players.size() >= MEETUP_MAX_PLAYERS:
				continue
			_meetup_players[peer] = clean_name(str(m.get("n", "")))
			meetup_player_joined.emit(peer, _meetup_info(peer, m))
	for peer in _meetup_players.keys():
		if not now_peers.has(peer):
			_meetup_players.erase(peer)
			meetup_player_left.emit(int(peer))


func _on_meetup_message(event: String, p: Dictionary) -> void:
	var peer := int(p.get("p", 0))
	if not _meetup_players.has(peer):
		return
	match event:
		"st":
			if not _rate_ok("st:%d" % peer, 150):
				return
			var v := [float(p.get("x", 0.0)), float(p.get("y", 0.0)), float(p.get("z", 0.0)),
					float(p.get("a", 0.0)), float(p.get("b", 0.0)), float(p.get("c", 0.0))]
			for n in v:
				if is_nan(n) or is_inf(n) or absf(n) > 20000.0:
					return
			meetup_state_received.emit(peer, Vector3(v[0], v[1], v[2]), Vector3(v[3], v[4], v[5]))
		"ch":
			if not _rate_ok("mc:%d" % peer, CHAT_RECEIVE_GAP_MS):
				return
			var text := clean_text(str(p.get("t", "")))
			if text != "":
				meetup_chat_received.emit(peer, str(_meetup_players[peer]), text)


## Called a few times a second by the Meetup. Sends nothing while alone, and little while standing still.
func publish_meetup_state(position: Vector3, rotation: Vector3) -> void:
	if not (online and in_meetup) or _meetup_players.is_empty():
		return
	var now := Time.get_ticks_msec()
	var moved := position.distance_to(_meetup_last_sent) > 0.25 or rotation.distance_to(_meetup_last_rot) > 0.03
	if not moved and now - _meetup_last_sent_ms < 3000:
		return
	_meetup_last_sent = position
	_meetup_last_rot = rotation
	_meetup_last_sent_ms = now
	_rt.send(_meetup_ch, "st", {"p": my_peer, "x": snappedf(position.x, 0.05), "y": snappedf(position.y, 0.05),
			"z": snappedf(position.z, 0.05), "a": snappedf(rotation.x, 0.01), "b": snappedf(rotation.y, 0.01),
			"c": snappedf(rotation.z, 0.01)})


## Meetup chat. Returns true when the message was sent (false: empty or still cooling down).
func send_meetup_chat(message: String) -> bool:
	var clean := clean_text(message)
	if clean == "":
		return false
	if not _chat_allowed():
		return false
	if online and in_meetup:
		_rt.send(_meetup_ch, "ch", {"p": my_peer, "t": clean})
	return true


func leave_meetup() -> void:
	if _meetup_ch != "":
		_rt.leave(_meetup_ch)
	_meetup_reset()


# ================================================================ leaderboard (Playgama board "total_wins")
func request_leaderboard() -> void:
	if Platform.leaderboard_type() == "in_game":
		var now := Time.get_ticks_msec()
		if now - _lb_fetch_ms < 60000:
			leaderboard_received.emit(Progress.get_leaderboard())   # asked again within a minute: no new request
			return
		_lb_fetch_ms = now
		Platform.fetch_leaderboard(func(list: Array):
			var mapped: Array = []
			for e in list:
				if e is Dictionary:
					mapped.append({"name": clean_name(str(e.get("name", "?"))), "wins": int(e.get("score", 0))})
			if not mapped.is_empty():
				Progress.remote_leaderboard = mapped
			leaderboard_received.emit(Progress.get_leaderboard()))
	else:
		leaderboard_received.emit(Progress.get_leaderboard())


## Kept for the match code: results are no longer sent anywhere from here (wins go to the Playgama board in progress_manager).
func report_result(_mode: String, _won: bool) -> void:
	pass


## The public page of YOUR game on Playgama - the link a friend opens to play. Paste it here once, e.g.
##   const GAME_PAGE_URL := "https://playgama.com/game/island-fight-race"
## (open your game on Playgama, copy the address from the browser bar). While this is empty the game tries to find the
## page address by itself and, if it cannot, falls back to the friend code text.
const GAME_PAGE_URL := "https://playgama.ai/play/vkfplf5vqin63f"


func game_url() -> String:
	var url := GAME_PAGE_URL.strip_edges()
	if url == "":
		url = invite_base_url.strip_edges()
	if url == "":
		url = _share_page_url()
	return url if url.begins_with("https://") else ""


## A link that opens the game and adds you as a friend (the 5-letter code travels in ?payload=).
func invite_link() -> String:
	var base := game_url()
	if base == "":
		return "My friend code: %s" % my_code
	return "%s%spayload=%s" % [base, "&" if base.contains("?") else "?", my_code]


## A link that opens the game and joins the custom room (the 6-letter Game ID travels in ?payload=).
func room_link() -> String:
	var rid := str(current_room.get("room_id", ""))
	var base := game_url()
	if base == "":
		return "Join my Island Fight Race match - Game ID: %s" % rid
	return "%s%spayload=%s" % [base, "&" if base.contains("?") else "?", rid]


## The public page of the game for Facebook: invite_base_url when set, otherwise the page that contains the game
## (the portal page the game runs on). Empty when nothing usable is known.
func _share_page_url() -> String:
	var url := invite_base_url.strip_edges()
	if url == "" and OS.has_feature("web"):
		var found = JavaScriptBridge.eval("(function(){try{if(window.location.ancestorOrigins&&window.location.ancestorOrigins.length&&document.referrer&&document.referrer.length>24){return document.referrer;}}catch(e){}return document.referrer||'';})()", true)
		url = str(found).strip_edges()
	return url if url.begins_with("https://") else ""


func race_join(key: String) -> bool:
	var ch := "r:" + key.left(40)
	if not online or _rt == null or not _rt.valid_name(ch):
		return false
	_race_ch = ch
	_rt.join(ch, false, {})
	return true


func race_ready() -> bool:
	return _race_ch != "" and _rt != null and _rt.is_joined(_race_ch)


func race_send(event: String, payload: Dictionary) -> bool:
	if _race_ch == "" or not online:
		return false
	return _rt.send(_race_ch, event, payload)


func race_leave() -> void:
	if _race_ch != "":
		_rt.leave(_race_ch)
		_race_ch = ""


## True when the hosting website forbids the connection to the online server (nothing can be done in code about that).
func policy_blocked() -> bool:
	return _rt != null and _rt.policy_blocked


## Copies text so it works inside a game portal's iframe (the plain clipboard call is often blocked there).
func copy_text(text: String) -> bool:
	if OS.has_feature("web"):
		var js := "(function(t){try{var a=document.createElement('textarea');a.value=t;a.setAttribute('readonly','');a.style.cssText='position:fixed;left:-9999px;top:0;opacity:0';document.body.appendChild(a);a.select();a.setSelectionRange(0,t.length);var ok=document.execCommand('copy');document.body.removeChild(a);if(!ok&&navigator.clipboard){navigator.clipboard.writeText(t).catch(function(){});return true;}return !!ok;}catch(e){return false;}})(%s)" % JSON.stringify(text)
		var ok = JavaScriptBridge.eval(js, true)
		if ok != null and bool(ok):
			return true
		DisplayServer.clipboard_set(text)
		return true
	DisplayServer.clipboard_set(text)
	return true


## Opens the system share sheet when the browser offers one, otherwise copies the link.
## (sms: / mailto: / WhatsApp / Facebook windows are blocked inside Playgama's iframe, so on the web only
## "share sheet" and "copy" exist; the other apps are still used on native builds.)
func share_invite(app: String) -> void:
	var is_room := app == "room"
	var link := room_link() if is_room else invite_link()
	var text := "Join me in Island Fight Race! " + link
	if OS.has_feature("web"):
		var js := "(function(t,u){try{if(navigator.share){navigator.share({title:'Island Fight Race',text:t,url:u}).catch(function(){});return 1;}}catch(e){}return 0;})(%s,%s)" % [JSON.stringify("Join me in Island Fight Race!"), JSON.stringify(link if link.begins_with("https://") else "")]
		var shared = JavaScriptBridge.eval(js, true)
		if shared != null and bool(shared):
			return
		copy_text(text)
		GameState.notify("Invitation copied - paste it to your friend")
		return


## Read-only counters of the realtime connection (used by the analytics system).
func net_stats() -> Dictionary:
	return (_rt.stats as Dictionary).duplicate() if _rt != null else {}


## The player changed the username: the open channels get the new name.
func refresh_name() -> void:
	if _rt == null or not online:
		return
	if _room_ready and _room_ch != "":
		var meta := {"c": my_code, "n": my_name(), "t": Time.get_unix_time_from_system()}
		if _room_host:
			meta["h"] = 1
		_rt.track(_room_ch, meta)
	if in_meetup and _meetup_ch != "":
		_rt.track(_meetup_ch, {"p": my_peer, "n": my_name(), "v": Progress.drivable_car()})


## The match starts: hands the lobby information to the game and opens the right map.
func _begin_match(info: Dictionary, type: String) -> void:
	GameState.game_mode = str(info.get("mode", "race"))
	GameState.match_type = type
	if type == "custom":
		# the host's choices apply to everybody in the room
		GameState.selected_mode = "advanced" if str(info.get("map", "simple")) == "advanced" else "simple"
		GameState.custom_laps = clampi(int(info.get("laps", 1)), 1, 5)
	GameState.match_info = {"room_id": info.get("room_id", ""), "players": int(info.get("count", 1)),
			"real_players": int(info.get("real_players", 1)), "names": info.get("players", []),
			"vehicles": info.get("vehicles", []), "seed": int(info.get("seed", 0))}
	match_found.emit(info)
