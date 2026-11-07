#!/usr/bin/env python3
"""Fail if the Godot project still references anything the slim template removes.

usage: python3 -I tools/audit_project.py <project_dir> [exported.pck] [--keep-mbedtls]
Scans .gd/.tscn/.tres/.cfg/.godot/.import text (and, optionally, the raw .pck, which also covers
the binary .scn/.gdc files made from your .glb models and scripts)."""
import json, os, re, sys

here = os.path.dirname(os.path.abspath(__file__))
args = [a for a in sys.argv[1:] if not a.startswith("--")]
keep_tls = "--keep-mbedtls" in sys.argv
if not args:
    sys.exit(__doc__)
proj = args[0]
pck = args[1] if len(args) > 1 else None

profile = json.load(open(os.path.join(here, "..", "profiles", "island_fight_race.build")))
banned = set(profile["disabled_classes"])
# classes that belong to disabled modules / options
banned |= {"CSGShape3D", "CSGBox3D", "CSGCombiner3D", "CSGCylinder3D", "CSGMesh3D", "CSGPolygon3D", "CSGSphere3D", "CSGTorus3D",
           "GridMap", "ENetConnection", "ENetMultiplayerPeer", "WebRTCPeerConnection", "WebRTCDataChannel", "WebRTCMultiplayerPeer",
           "UPNP", "UPNPDevice", "SceneMultiplayer", "MultiplayerSpawner", "MultiplayerSynchronizer", "CameraServer", "CameraFeed",
           "OpenXRInterface", "WebXRInterface", "XRServer", "XROrigin3D", "XRCamera3D", "XRController3D", "VideoStreamTheora",
           "NoiseTexture2D", "NoiseTexture3D", "FastNoiseLite", "GLTFDocument", "GLTFState", "FBXDocument", "JSONRPC",
           "NavigationServer2D", "NavigationServer3D", "NavigationRegion3D", "NavigationAgent3D", "NavigationAgent2D"}
banned |= {"@rpc", "MultiplayerAPI"}
if not keep_tls:
    banned |= {"Crypto", "CryptoKey", "X509Certificate", "HMACContext", "AESContext", "StreamPeerTLS", "PacketPeerDTLS", "DTLSServer"}

word = re.compile(r"(?<![A-Za-z0-9_])(" + "|".join(re.escape(b) for b in sorted(banned, key=len, reverse=True)) + r")(?![A-Za-z0-9_])")
hits = {}
def scan(name, data):
    for m in word.finditer(data):
        hits.setdefault(m.group(1), set()).add(name)

for root, dirs, files in os.walk(proj):
    dirs[:] = [d for d in dirs if d != ".git"]
    for f in files:
        if f.endswith((".gd", ".tscn", ".tres", ".cfg", ".godot", ".import", ".json", ".html", ".js")):
            p = os.path.join(root, f)
            # comments are fine: skip pure comment lines in scripts
            txt = open(p, encoding="utf-8", errors="ignore").read()
            if f.endswith(".gd"):
                txt = "\n".join(l for l in txt.splitlines() if not l.lstrip().startswith("#"))
                txt = re.sub(r'"[^"\n]*"', '""', txt)  # ignore text inside strings
            scan(os.path.relpath(p, proj), txt)
if pck:
    scan(os.path.basename(pck), open(pck, "rb").read().decode("latin-1"))

if hits:
    print("BLOCKED - these removed classes are still referenced (remove them from the profile or the project):")
    for k in sorted(hits):
        print("  %-28s %s" % (k, ", ".join(sorted(hits[k])[:4])))
    sys.exit(1)
print("OK - nothing the slim template removes is referenced by the project%s." % (" or the PCK" if pck else ""))
