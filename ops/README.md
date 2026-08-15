# Automatic disk capacity management

This directory is the source-controlled copy of the capacity manager installed
on `busanlocalproducts` on 2026-08-13.

## Runtime mapping

| Repository path | Server path |
|---|---|
| `ops/busan_disk_capacity_manager.py` | `/usr/local/sbin/busan-disk-capacity-manager.py` |
| `ops/systemd/busan-disk-capacity-manager.service` | `/etc/systemd/system/busan-disk-capacity-manager.service` |
| `ops/systemd/busan-disk-capacity-manager.timer` | `/etc/systemd/system/busan-disk-capacity-manager.timer` |
| server secret only | `/etc/busan-capacity-manager.env` |

Do not commit `/etc/busan-capacity-manager.env`. It contains the Object Storage
credentials and must remain `root:root` with mode `0600`.

## Install and verify

```bash
cd /opt/busan
sudo bash ops/install_capacity_manager.sh
sudo bash ops/verify_capacity_manager.sh
sudo journalctl -u busan-disk-capacity-manager.service -n 100 --no-pager
```

The manager starts cleanup at 80%, enables verified Object Storage migration at
85%, and performs urgent cleanup at 90%. Uploads are checked by remote size and
SHA-256 metadata before the local source is removed. A failed upload or failed
verification leaves the local source in place.

For the complete retention policy, protected paths, and recovery commands, read
the private integrated handover in `doors1118-sketch/busan-guarantee-dashboard`,
branch `codex/cloud-continuity-20260813`, section 13.1.
