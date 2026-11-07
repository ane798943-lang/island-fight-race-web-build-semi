# custom.py - auto-loaded by Godot's SConstruct when placed in the Godot source root.
# Target: Island Fight Race, Web, single-thread, GL Compatibility, GDScript, Jolt, no GDExtension.
# Anything SCons does not recognise for your Godot version is only printed as a warning.

# ---- build kind -------------------------------------------------------------
platform = "web"
target = "template_release"
threads = False            # export preset: thread_support=false
dlink_enabled = False      # export preset: extensions_support=false
javascript_eval = True     # REQUIRED: the game calls JavaScriptBridge.eval() ~21 times
build_name = "custom_build"

# ---- size ------------------------------------------------------------------
optimize = "size"          # -Os. "size_extra" (-Oz) is smaller but slower on low-end phones
lto = "full"
production = True
debug_symbols = False
use_assertions = False
deprecated = False         # drops Godot 3-style compatibility shims (KinematicBody, StreamTexture, ...)

# ---- renderer: GL Compatibility only (project.godot) -----------------------------
opengl3 = True
vulkan = False
d3d12 = False
metal = False
openxr = False
use_volk = False

# ---- engine features the game never uses -------------------------------------
minizip = False            # no ZIPReader / ZIPPacker use
brotli = False             # only needed for WOFF2 fonts
graphite = False
accesskit = False
disable_xr = True
disable_navigation_2d = True
disable_navigation_3d = True
disable_physics_2d = True

# ---- modules ---------------------------------------------------------------
# KEPT on purpose: gdscript, jolt_physics, websocket, freetype, text_server_fb, vorbis, ogg,
#                  webp (3 textures are WebP), jpg (avatar upload), etcpak/astcenc/bcdec/cvtt (VRAM textures)
for _m in (
    "csg", "gridmap", "enet", "webrtc", "upnp", "multiplayer", "camera", "mobile_vr",
    "openxr", "webxr", "theora", "basis_universal", "gltf", "fbx", "noise", "jsonrpc",
    "tga", "bmp", "dds", "ktx", "hdr", "tinyexr", "msdfgen", "godot_physics_2d", "godot_physics_3d",
    "navigation_2d", "navigation_3d", "mono", "interactive_music", "minimp3", "raycast", "vhacd", "squish",
):
    globals()["module_%s_enabled" % _m] = False

# Needs the small script patch in patches/ (replaces Crypto with RandomNumberGenerator).
# Set to True to KEEP mbedtls (e.g. if you did not apply the patch).
KEEP_MBEDTLS = False
module_mbedtls_enabled = KEEP_MBEDTLS

# Extra savings that need an in-browser test before shipping (default: kept ON = safe):
#   module_svg_enabled = False     # default Control theme icons are SVG; your UI draws its own, but test CheckButton/HSlider/OptionButton
#   module_regex_enabled = False   # unused by your scripts

build_profile = "profiles/island_fight_race.build"
