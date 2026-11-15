"""Generate a VAPID key pair for Web Push, printed as .env lines.

Run once, then paste the output into your .env (and restart the server):

    python generate_vapid_keys.py

The PUBLIC key is also what the browser uses as its applicationServerKey;
the dashboard fetches it from /api/push/vapid-public-key, so you only need
to set these two env vars.
"""

import base64

from cryptography.hazmat.primitives.asymmetric import ec


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def main():
    private_key = ec.generate_private_key(ec.SECP256R1())
    public_key = private_key.public_key()

    # Raw 32-byte private scalar (base64url) — accepted by pywebpush/py_vapid.
    priv_value = private_key.private_numbers().private_value
    priv_raw = priv_value.to_bytes(32, "big")

    # Uncompressed public point (0x04 || X || Y), base64url — the applicationServerKey.
    from cryptography.hazmat.primitives import serialization
    pub_raw = public_key.public_bytes(
        serialization.Encoding.X962,
        serialization.PublicFormat.UncompressedPoint,
    )

    print("# Add these to your .env file, then restart the server:")
    print(f"VAPID_PRIVATE_KEY={_b64url(priv_raw)}")
    print(f"VAPID_PUBLIC_KEY={_b64url(pub_raw)}")


if __name__ == "__main__":
    main()
