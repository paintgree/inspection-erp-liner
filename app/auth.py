import hashlib
import hmac

# Simple deterministic hash for demo (no bcrypt issues on Render)
# For production we will switch back to bcrypt.
_SECRET = b"demo-secret-change-later"

def hash_password(pw: str) -> str:
    msg = pw.encode("utf-8")
    return hmac.new(_SECRET, msg, hashlib.sha256).hexdigest()

def verify_password(pw: str, hashed: str) -> bool:
    return hmac.compare_digest(hash_password(pw), hashed)
