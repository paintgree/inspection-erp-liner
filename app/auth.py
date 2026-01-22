import hashlib
import hmac

_SECRET = b"demo-secret-change-later"

def hash_password(pw: str) -> str:
    return hmac.new(_SECRET, pw.encode("utf-8"), hashlib.sha256).hexdigest()

def verify_password(pw: str, hashed: str) -> bool:
    return hmac.compare_digest(hash_password(pw), hashed)
