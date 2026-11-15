#!/bin/sh
# Parquet Opener launcher, invoked by the AppleScript droplet with dropped
# files as arguments. Forwards them as argv to the real viewer binary.
# (Kept as a shell script so quoting stays sane -- AppleScript string escapes
# for embedded double quotes proved unreliable via osacompile.)
#
# $0 = <App>/Contents/Helpers/Parquet Viewer.app/Contents/Resources/launch.sh
set -u
APP_CONTENTS="${0%/Helpers/*}"
APP_BUNDLE="${APP_CONTENTS%/Contents}"
# Hand the launch to LaunchServices instead of forking the viewer. A forked
# child stays in the droplet's coalition, so LaunchServices keeps the droplet
# registered as a running app -- no process, no window -- and the Dock shows a
# second tile, "Parquet Viewer (Running in background)". `open -n` creates a
# new instance owned by launchd, so the droplet exits with no subordinates.
if [ "$#" -eq 0 ]; then
    open -n -a "$APP_BUNDLE"
else
    for f in "$@"; do
        open -n -a "$APP_BUNDLE" --args "$f"
    done
fi
