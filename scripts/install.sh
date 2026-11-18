#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
DESTDIR=${DESTDIR:-}
if [ -z "$DESTDIR" ]; then
    [ "$(uname -s)" = Linux ] && [ "$(id -u)" = 0 ] || {
        echo 'Live installation requires Linux and root; use DESTDIR to stage files.' >&2
        exit 1
    }
fi
service=relayd
binaries='relayd'
config=service.env
for binary in $binaries; do
    [ -f "bin/$binary" ] || { echo "Missing bin/$binary; run make build first." >&2; exit 1; }
done
install -d "$DESTDIR/usr/local/bin" "$DESTDIR/etc/systemd/system"
install -d -m 0750 "$DESTDIR/etc/$service"
for binary in $binaries; do
    install -m 0755 "bin/$binary" "$DESTDIR/usr/local/bin/$binary"
done
install -m 0644 "packaging/systemd/$service.service" "$DESTDIR/etc/systemd/system/$service.service"
source="packaging/config/$service.$config"
target="$DESTDIR/etc/$service/$config"
if [ -e "$target" ]; then
    install -m 0640 "$source" "$target.example"
    echo "Preserved $target; defaults saved as $target.example"
else
    install -m 0640 "$source" "$target"
fi
if [ -z "$DESTDIR" ]; then
    systemctl daemon-reload
    echo "Installed. Configure /etc/$service as applicable, then enable selected services."
else
    echo "Files staged under $DESTDIR; host services were not changed."
fi
