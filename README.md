# Island Fight Race - slim Web template kit

1. Create a GitHub repo, upload everything in this folder (keep `.github/`).
2. Actions -> "Build slim Godot Web template" -> Run workflow (set the Emscripten version your current template used).
3. Download `web_nothreads_release.zip` + `sizes.txt`.
4. Godot -> Export -> Web -> Custom Template -> Release = that zip. Thread support OFF, Extensions OFF.
5. Apply `patches/0001-remove-crypto-dependency.patch` to the game first (or copy `patches/patched_files/*`), or set `KEEP_MBEDTLS = True` in `custom.py`.
6. Before building, run `python3 -I tools/audit_project.py <your_project_folder>` - it fails if a removed class is still used.

Test in a real browser (desktop + low-end phone): menu, race, online lobby, Firebase login, ads, profile photo picker, audio.
