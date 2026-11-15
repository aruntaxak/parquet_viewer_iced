#!/bin/sh
# Assemble /Applications/Parquet Viewer.app from the release binary, plus the
# Parquet Viewer helper droplet (owns the .parquet association and forwards
# files via argv -- winit-based apps can't receive cold-launch odoc events).
# Usage: ./scripts/bundle_macos.sh [/path/to/Parquet Viewer.app]
set -eu
cd "$(dirname "$0")/.."

APP="${1:-/Applications/Parquet Viewer.app}"
# The droplet is the only .parquet handler, so it carries the app's name: the
# Open With menu must show a single "Parquet Viewer" entry.
HELPER="$APP/Contents/Helpers/Parquet Viewer.app"

cargo build --release

# --- main app bundle ---
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources" "$APP/Contents/Helpers"
cp target/release/parquet-viewer "$APP/Contents/MacOS/parquet-viewer"
cp bundle/Info.plist "$APP/Contents/Info.plist"
cp bundle/parquet-viewer.icns "$APP/Contents/Resources/parquet-viewer.icns"
cp bundle/parquet-doc.icns "$APP/Contents/Resources/parquet-doc.icns"

# --- helper droplet (AppleScript, owns .parquet) ---
rm -rf "$HELPER"
osacompile -o "$HELPER" bundle/opener.applescript
# osacompile ships a generic droplet icon in Assets.car and names it via
# CFBundleIconName, which outranks CFBundleIconFile. Drop both so the parquet
# icon below is the only icon the bundle can resolve.
rm -f "$HELPER/Contents/Resources/Assets.car" "$HELPER/Contents/Resources/droplet.icns"
cp bundle/parquet-viewer.icns "$HELPER/Contents/Resources/parquet-viewer.icns"
cp bundle/parquet-doc.icns "$HELPER/Contents/Resources/parquet-doc.icns"
cp bundle/launch.sh "$HELPER/Contents/Resources/launch.sh"
chmod +x "$HELPER/Contents/Resources/launch.sh"
python3 - "$HELPER/Contents/Info.plist" <<'EOF'
import plistlib, sys
p = sys.argv[1]
with open(p, 'rb') as f:
    d = plistlib.load(f)
d['CFBundleIdentifier'] = 'com.aruntaxak.parquet-viewer.opener'
d['CFBundleDisplayName'] = 'Parquet Viewer'
d['CFBundleName'] = 'Parquet Viewer'
d['CFBundleIconFile'] = 'parquet-viewer'
d.pop('CFBundleIconName', None)
d['LSMinimumSystemVersion'] = '10.13'
d['NSHighResolutionCapable'] = True
d['CFBundleDocumentTypes'] = [{
    'CFBundleTypeName': 'Parquet File',
    'CFBundleTypeRole': 'Viewer',
    'LSHandlerRank': 'Owner',
    'LSItemContentTypes': ['org.apache.parquet'],
    # Finder takes the document icon from the handler's document type, not
    # from UTTypeIconFile on the exporting bundle.
    'CFBundleTypeIconFile': 'parquet-doc',
}]
with open(p, 'wb') as f:
    plistlib.dump(d, f)
EOF

codesign --force --deep --sign - "$APP"
codesign --verify --deep --strict "$APP"
# Bump the helper's mtime so the icon cache re-reads the bundle instead of
# serving the icon it cached for the previous build.
touch "$HELPER"
/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister -f "$HELPER"
/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister -f "$APP"

echo "Installed: $APP"
echo "Helper:    $HELPER"
