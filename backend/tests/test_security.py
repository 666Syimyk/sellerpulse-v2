from utils.security import hash_password, verify_password, create_access_token, decode_access_token, encrypt_text, decrypt_text


def test_hash_and_verify():
    h = hash_password("secret123")
    assert verify_password("secret123", h)
    assert not verify_password("wrong", h)


def test_hash_different_salts():
    h1 = hash_password("same")
    h2 = hash_password("same")
    assert h1 != h2
    assert verify_password("same", h1)
    assert verify_password("same", h2)


def test_jwt_roundtrip():
    token = create_access_token("42")
    assert decode_access_token(token) == "42"


def test_encrypt_decrypt():
    original = "my-wb-api-token-value"
    encrypted = encrypt_text(original)
    assert encrypted != original
    assert decrypt_text(encrypted) == original


def test_encrypt_different_ciphertexts():
    e1 = encrypt_text("same")
    e2 = encrypt_text("same")
    assert e1 != e2
    assert decrypt_text(e1) == "same"
    assert decrypt_text(e2) == "same"
