# ============================================================
# Smart Telegram Scanner v1.11 (FIXED & DROP-IN)
# Detects shared/forwarded posts → marks download status
# Fully compatible with: download_manager, reupload_manager,
# file_handler, utils, progress_bar, uploader v4.32+
# ============================================================

from telethon.tl.types import Message, ChatInviteAlready
from telethon.errors import RPCError, UserAlreadyParticipantError, InviteHashInvalidError
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.tl.functions.channels import GetAdminLogRequest
from telethon.tl.types import ChannelAdminLogEventsFilter, ChannelAdminLogEventActionDeleteMessage

from core.file_handler import is_downloaded
from pathlib import Path
import json
import time


def _extract_forward_ids(msg):
    """Return (original_source_ids_set, forward_sender_ids_set).

    This helper tries multiple attribute patterns that Telethon may expose
    for forwarded messages and normalizes them to integer IDs.
    """
    orig_ids = set()
    sender_ids = set()

    fwd = getattr(msg, "fwd_from", None) or getattr(msg, "forward", None)
    if not fwd:
        return orig_ids, sender_ids

    # Fields that may reference the original peer
    candidates = [
        getattr(fwd, "from_id", None),
        getattr(fwd, "channel_id", None),
        getattr(fwd, "sender_id", None),
        getattr(fwd, "user_id", None),
    ]

    for c in candidates:
        if c is None:
            continue
        # If it's an int-like
        try:
            if isinstance(c, (int,)) or (isinstance(c, str) and c.lstrip("-").isdigit()):
                orig_ids.add(int(c))
                continue
        except Exception:
            pass

        # If it's a TL object with .user_id or .channel_id
        try:
            sub = getattr(c, "user_id", None) or getattr(c, "channel_id", None)
            if sub is not None:
                orig_ids.add(int(sub))
                continue
        except Exception:
            pass

    # Forward sender (the account who forwarded) may be present as .sender_id or .from_id
    try:
        fs = getattr(fwd, "sender_id", None) or getattr(fwd, "from_id", None)
        if fs is not None:
            if isinstance(fs, int) or (isinstance(fs, str) and fs.lstrip("-").isdigit()):
                sender_ids.add(int(fs))
            else:
                sub = getattr(fs, "user_id", None) or getattr(fs, "channel_id", None)
                if sub is not None:
                    sender_ids.add(int(sub))
    except Exception:
        pass

    # Also check top-level message fields as fallback
    try:
        if getattr(msg, "forward", None) is not None:
            top = getattr(msg.forward, "from_id", None) or getattr(msg.forward, "sender_id", None)
            if top is not None:
                if isinstance(top, int) or (isinstance(top, str) and top.lstrip("-").isdigit()):
                    orig_ids.add(int(top))
                else:
                    sub = getattr(top, "user_id", None) or getattr(top, "channel_id", None)
                    if sub is not None:
                        orig_ids.add(int(sub))
    except Exception:
        pass

    return orig_ids, sender_ids

# ───────────────────────────────
# CONFIG
# ───────────────────────────────
DEBUG = False  # Set True to see debug logs


# ───────────────────────────────
# HELPER: Resolve any source entity
# ───────────────────────────────
async def _resolve_source(client, source):
    """
    Resolve source into a Telegram entity.
    Supports:
    - username
    - numeric ID (int or numeric string)
    - -100xxxxxxxxxx channel IDs
    - invite links (t.me/+xxxx, t.me/joinchat/xxxx)
    """
    if isinstance(source, str):
        source = source.strip()

        if source.isdigit():
            source = int(source)
        elif source.startswith("-100") and source[1:].isdigit():
            source = int(source)

    # Invite links
    if isinstance(source, str) and source.startswith("https://t.me/"):
        if "/+" in source or "/joinchat/" in source:
            invite_hash = source.rstrip("/").split("/")[-1].lstrip("+")
            try:
                if DEBUG:
                    print(f"[Scanner] Resolving invite hash: {invite_hash}")

                invite = await client(CheckChatInviteRequest(hash=invite_hash))
                if isinstance(invite, ChatInviteAlready):
                    if DEBUG:
                        print("[Scanner] Already joined via invite.")
                    return invite.chat

                if DEBUG:
                    print("[Scanner] Joining chat via invite.")
                updates = await client(ImportChatInviteRequest(hash=invite_hash))

                if not updates.chats:
                    raise RuntimeError("Invite join succeeded but no chats returned")
                return updates.chats[0]

            except InviteHashInvalidError:
                print(f"[Scanner] Invalid invite link: {invite_hash}")
                raise
            except UserAlreadyParticipantError:
                if DEBUG:
                    print("[Scanner] Already participant (fallback).")
                return await client.get_entity(source)
            except Exception as e:
                print(f"[Scanner] Failed to resolve invite link: {e}")
                raise

    try:
        return await client.get_entity(source)
    except Exception as e:
        print(f"[Scanner] Failed to resolve entity '{source}': {e}")
        raise


# ───────────────────────────────
# MAIN SCANNER
# ───────────────────────────────
async def scan_shared_messages(client, source, limit=None):
    """
    Scan a source channel for forwarded/shared media messages
    that are not yet downloaded.

    Returns:
        List[Message] ordered from oldest → newest
    """
    results = []
    total_forwarded = 0

    try:
        entity = await _resolve_source(client, source)

        # Load exclusion list (optional). File: config/scanner_exclude.json
        # Expected format: ["@username", "https://t.me/xyz", "-1001234567890", "12345678"]
        exclude_file = Path(__file__).parent.parent / "config" / "scanner_exclude.json"
        excluded_ids = set()
        if exclude_file.exists():
            try:
                from core.utils import _safe_json_loads
                raw = _safe_json_loads(exclude_file.read_text(encoding="utf-8"))
                if isinstance(raw, list):
                    for item in raw:
                        s = str(item).strip()
                        if not s:
                            continue
                        # numeric IDs
                        if s.lstrip("-").isdigit():
                            try:
                                excluded_ids.add(int(s))
                                continue
                            except Exception:
                                pass
                        # attempt to resolve via Telegram (username, t.me links)
                        try:
                            ent = await client.get_entity(s)
                            if ent is not None:
                                eid = getattr(ent, "id", None) or getattr(ent, "channel_id", None)
                                if eid is not None:
                                    excluded_ids.add(int(eid))
                        except Exception:
                            # ignore unresolved entries
                            if DEBUG:
                                print(f"[Scanner] Unable to resolve exclude entry: {s}")
                            continue
            except Exception as e:
                if DEBUG:
                    print(f"[Scanner] Failed reading exclude file: {e}")

        # Always print loaded excludes
        if excluded_ids:
            print(f"[Scanner] ✓ Loaded {len(excluded_ids)} excluded IDs: {sorted(excluded_ids)}")
        else:
            print("[Scanner] ℹ No excluded IDs loaded from scanner_exclude.json")

        if DEBUG:
            me = await client.get_me()
            print(f"[Scanner] Using account: {me.username or me.id}")

        async for msg in client.iter_messages(entity, reverse=True, limit=limit):
            try:
                if not isinstance(msg, Message):
                    continue
                if msg.action is not None:  # Skip service messages
                    continue
                if msg.from_id is not None:  # Skip explicit user posts
                    continue
                if not msg.media and not msg.text:  # Skip empty
                    continue

                # Detect forwarded/shared
                is_forwarded = bool(getattr(msg, "forward", None) or getattr(msg, "fwd_from", None))
                if not is_forwarded:
                    continue

                # Write forward debug info when excludes are configured (helps diagnose matching issues)
                try:
                    if excluded_ids:
                        dbg = {
                            "ts": time.strftime('%Y-%m-%d %H:%M:%S'),
                            "msg_id": getattr(msg, 'id', None),
                            "fwd_from": str(getattr(msg, 'fwd_from', None)),
                            "forward": str(getattr(msg, 'forward', None)),
                        }
                        fdbg = Path(__file__).parent.parent / "config" / "scanner_forward_debug.log"
                        with fdbg.open('a', encoding='utf-8') as fh:
                            fh.write(json.dumps(dbg, ensure_ascii=False) + "\n")
                except Exception:
                    pass

                # Determine original & sender IDs robustly and skip if excluded
                try:
                    orig_ids, sender_ids = _extract_forward_ids(msg)

                    ent_id = getattr(entity, "id", None) or getattr(entity, "channel_id", None)

                    # Log extraction if excludes are configured (helps diagnose matching)
                    if excluded_ids and (orig_ids or sender_ids):
                        print(f"[Scanner] Forward msg={msg.id}: orig_ids={orig_ids}, sender_ids={sender_ids}, excluded={excluded_ids}")

                    # If forwarded from the same entity, skip
                    if ent_id is not None and any((int(x) == int(ent_id)) for x in orig_ids if x is not None):
                        if DEBUG:
                            print(f"[Scanner] Skipping internal forward (original source == scanned entity): {msg.id}")
                        # log skipped
                        try:
                            logf = Path(__file__).parent.parent / "config" / "scanner_skipped.log"
                            logf.write_text(logf.read_text(encoding="utf-8") + f"{time.strftime('%Y-%m-%d %H:%M:%S')} SKIP_INTERNAL {msg.id} {ent_id}\n", encoding="utf-8")
                        except Exception:
                            pass
                        continue

                    # If any original id is excluded, skip
                    if any((int(x) in excluded_ids) for x in orig_ids if x is not None):
                        print(f"[Scanner] ✓ SKIP forward from excluded source: msg={msg.id}, orig_ids={orig_ids}")
                        try:
                            logf = Path(__file__).parent.parent / "config" / "scanner_skipped.log"
                            logf.write_text(logf.read_text(encoding="utf-8") + f"{time.strftime('%Y-%m-%d %H:%M:%S')} SKIP_SOURCE {msg.id} {list(orig_ids)}\n", encoding="utf-8")
                        except Exception:
                            pass
                        continue

                    # If forward sender is excluded, skip
                    if any((int(x) in excluded_ids) for x in sender_ids if x is not None):
                        print(f"[Scanner] ✓ SKIP forward because sender is excluded: msg={msg.id}, sender_ids={sender_ids}")
                        try:
                            logf = Path(__file__).parent.parent / "config" / "scanner_skipped.log"
                            logf.write_text(logf.read_text(encoding="utf-8") + f"{time.strftime('%Y-%m-%d %H:%M:%S')} SKIP_SENDER {msg.id} {list(sender_ids)}\n", encoding="utf-8")
                        except Exception:
                            pass
                        continue
                except Exception:
                    # Fallback: treat as forwarded
                    pass
                total_forwarded += 1

                chat_id = msg.chat_id or getattr(msg.to_id, "channel_id", None)
                if chat_id is None:
                    continue

                # Skip already downloaded
                if is_downloaded(msg.id, chat_id):
                    if DEBUG:
                        print(f"[Scanner] Skipping already downloaded: {msg.id}")
                    continue

                # Archive message for guaranteed recovery
                from core.archive_manager import archive_message_during_relay
                archive_message_during_relay(msg, f"{chat_id}__TO__{chat_id}")

                results.append(msg)

            except Exception as per_msg_error:
                if DEBUG:
                    print(f"[Scanner] Skipped message due to error: {per_msg_error}")
                continue

    except RPCError as e:
        print(f"[Scanner Error] Telegram RPC Error: {e}")
    except Exception as e:
        print(f"[Scanner Error] Unexpected error: {e}")

    print(f"[Scanner] Total forwarded/shared posts detected: {total_forwarded}")
    if DEBUG:
        print(f"[Scanner] Found {len(results)} new shared messages.")
    return results


# ───────────────────────────────
# ADMIN LOG SCANNER FOR DELETED MESSAGES
# ───────────────────────────────
async def scan_deleted_messages(client, source, limit=None, max_id=None, min_id=None, forwarded_only=False, ignore_downloaded=False):
    """
    Scan a source channel's admin log for deleted messages that contain media
    or text and have not yet been processed.  The caller must have admin rights
    on *source* to read the admin log.

    Parameters
    ----------
    client
        Telethon client to use for the request.
    source
        Channel/entity to inspect.
    limit
        Maximum number of admin log events to fetch in one batch.
    max_id
        (optional) admin log event ID used for pagination.  Pass the value
        returned as ``min_event_id`` from a previous call minus one to obtain
        older events.
    min_id
        (optional) **message** ID filter.  Any deleted messages with an ID
        lower than this value will be skipped; this is *not* sent to the
    ignore_downloaded
        If True, ignore progress.json when deciding whether a deleted message
        has already been handled. Useful when forcing a re-recovery starting
        from a particular ID.
        Telegram API and only applied locally after events are retrieved.
    forwarded_only
        If True, only consider deleted messages that were forwarded/shared.

    Returns
    -------
    Tuple[List[Message], Optional[int], Optional[int]]
        A list of recoverable deleted messages, sorted oldest→newest,
        the smallest admin log event ID seen (useful for pagination), and the
        smallest **message** ID encountered across all events.  The latter can
        be used by callers to determine whether the admin log contains any
        deletions older than a requested `min_id` filter.
    """
    results = []
    total_deleted = 0
    # track the smallest message ID seen in the events we fetch
    min_msg_id_seen = None

    try:
        entity = await _resolve_source(client, source)

        # Check if we have admin rights
        try:
            me = await client.get_me()
            admin_rights = await client.get_permissions(entity, me)
            print(f"[AdminLog] Admin rights check: is_admin={admin_rights.is_admin}, can_delete_messages={getattr(admin_rights, 'delete_messages', False)}")
            if not admin_rights.is_admin:
                print(f"[AdminLog] No admin rights for {entity.id}, cannot access admin log")
                return []
        except Exception as e:
            print(f"[AdminLog] Cannot check admin rights: {e}")
            return []

        # Load exclusion list
        exclude_file = Path(__file__).parent.parent / "config" / "scanner_exclude.json"
        excluded_ids = set()
        if exclude_file.exists():
            try:
                from core.utils import _safe_json_loads
                raw = _safe_json_loads(exclude_file.read_text(encoding="utf-8"))
                if isinstance(raw, list):
                    for item in raw:
                        s = str(item).strip()
                        if not s:
                            continue
                        if s.lstrip("-").isdigit():
                            try:
                                excluded_ids.add(int(s))
                                continue
                            except Exception:
                                pass
                        try:
                            ent = await client.get_entity(s)
                            if ent is not None:
                                eid = getattr(ent, "id", None) or getattr(ent, "channel_id", None)
                                if eid is not None:
                                    excluded_ids.add(int(eid))
                        except Exception:
                            if DEBUG:
                                print(f"[AdminLog] Unable to resolve exclude entry: {s}")

            except Exception as e:
                if DEBUG:
                    print(f"[AdminLog] Error loading exclusions: {e}")

        # Create filter for deleted messages only
        events_filter = ChannelAdminLogEventsFilter(delete=True)  # Only deleted messages

        print(f"[AdminLog] Scanning admin log for deleted messages in {entity.id}...")
        print(f"[AdminLog] Channel type: {type(entity).__name__}")
        print(f"[AdminLog] Channel ID: {entity.id}")

        # Get admin log events
        # Note: `max_id` and `min_id` in the Telegram API refer to **admin log event IDs**,
        # not message IDs.  `max_id` is used for pagination (events older than this value),
        # and `min_id` is typically left at 0 when scanning backwards.
        # We accept a `max_id` argument from the caller so recover_deleted_posts can
        # walk the log backwards; any `min_id` argument passed in is treated as a
        # **message ID filter** later on and therefore is *not* sent to the API.
        request_kwargs = {
            'channel': entity,
            'q': "",
            'events_filter': events_filter,
            'limit': limit or 100,
            # use the supplied max_id for paging, defaulting to 0 (most recent events)
            'max_id': max_id or 0,
            # the API requires a min_id parameter; we always set it to 0 here
            'min_id': 0
        }

        try:
            admin_log = await client(GetAdminLogRequest(**request_kwargs))
        except Exception as e:
            print(f"[AdminLog] Failed to get admin log with delete filter: {e}")
            print(f"[AdminLog] Trying without filter...")
            # Try without filter
            request_kwargs['events_filter'] = None
            admin_log = await client(GetAdminLogRequest(**request_kwargs))

        for event in admin_log.events:
            print(f"[AdminLog] Event ID: {event.id}, Action type: {type(event.action).__name__}")
            # When using delete filter, all events should be delete events
            if not isinstance(event.action, ChannelAdminLogEventActionDeleteMessage):
                print(f"[AdminLog] Unexpected non-delete event: {type(event.action).__name__}")
                continue

            # The deleted message is in event.action.message
            deleted_msg = event.action.message
            print(f"[AdminLog] Event {event.id}: message is None: {deleted_msg is None}")
            if not deleted_msg:
                print(f"[AdminLog] Delete event {event.id} has no message data")
                continue

            # record smallest message id seen regardless of filtering
            try:
                mid = int(deleted_msg.id)
                if min_msg_id_seen is None or mid < min_msg_id_seen:
                    min_msg_id_seen = mid
            except Exception:
                pass

            # Filter by message ID if min_id is specified (only recover messages with ID >= min_id)
            if min_id is not None and deleted_msg.id < min_id:
                if DEBUG:
                    print(f"[AdminLog] Skipping message {deleted_msg.id} - below min_id filter {min_id}")
                continue

            total_deleted += 1

            print(f"[AdminLog] Found deleted message ID {deleted_msg.id}, has_media={bool(getattr(deleted_msg, 'media', None))}, has_text={bool(getattr(deleted_msg, 'text', None))}")

            # Check if it's a forwarded/shared message (same logic as regular scanner)
            is_forwarded = bool(getattr(deleted_msg, "forward", None) or getattr(deleted_msg, "fwd_from", None))
            if forwarded_only and not is_forwarded:
                print(f"[AdminLog] Skipping message {deleted_msg.id} - not forwarded")
                continue

            # Check exclusions
            if excluded_ids:
                try:
                    orig_ids, sender_ids = _extract_forward_ids(deleted_msg)
                    if any((int(x) in excluded_ids) for x in orig_ids if x is not None):
                        if DEBUG:
                            print(f"[AdminLog] SKIP excluded source: deleted post {deleted_msg.id}, orig_ids={orig_ids}")
                        continue
                    if any((int(x) in excluded_ids) for x in sender_ids if x is not None):
                        if DEBUG:
                            print(f"[AdminLog] SKIP excluded sender: deleted post {deleted_msg.id}, sender_ids={sender_ids}")
                        continue
                except Exception as e:
                    if DEBUG:
                        print(f"[AdminLog] Error checking exclusions: {e}")

            # Check if already downloaded (unless override requested)
            progress_key = f"{entity.id}__TO__{entity.id}"  # Assuming same channel for now
            if not ignore_downloaded and is_downloaded(deleted_msg.id, progress_key):
                if DEBUG:
                    print(f"[AdminLog] Skipping already downloaded deleted message: {deleted_msg.id}")
                continue

            # Check if has media or text
            if not (getattr(deleted_msg, "media", None) or getattr(deleted_msg, "text", None)):
                print(f"[AdminLog] Skipping message {deleted_msg.id} - no media or text")
                continue

            results.append(deleted_msg)
            if DEBUG:
                print(f"[AdminLog] Found recoverable deleted message: {deleted_msg.id}")

    except RPCError as e:
        print(f"[AdminLog Error] Telegram RPC Error: {e}")
        print("This might be because you don't have admin rights or the channel doesn't support admin logs")
    except Exception as e:
        print(f"[AdminLog Error] Unexpected error: {e}")

    print(f"[AdminLog] Total deleted messages detected: {total_deleted}")
    print(f"[AdminLog] Found {len(results)} recoverable deleted messages with media/text")
    
    # Return min event ID for pagination (smallest event ID in this batch)
    min_event_id = min((event.id for event in admin_log.events), default=None) if hasattr(admin_log, 'events') and admin_log.events else None
    # also return the lowest message id we observed (None if we saw no events)
    return results, min_event_id, min_msg_id_seen


# ───────────────────────────────
# TEST / STANDALONE
# ───────────────────────────────
if __name__ == "__main__":
    import asyncio
    from telethon import TelegramClient

    import os
    API_ID = int(os.getenv("API_ID", "123456"))
    API_HASH = os.getenv("API_HASH", "your_api_hash_here")
    PHONE = os.getenv("PHONE", "+1234567890")

    DEBUG = True

    async def test_scan():
        client = TelegramClient("scanner_session", API_ID, API_HASH)
        await client.start(PHONE)
        msgs = await scan_shared_messages(client, "https://t.me/+xxxx")
        print(f"Detected {len(msgs)} forwarded/shared messages")
        await client.disconnect()

    asyncio.run(test_scan())