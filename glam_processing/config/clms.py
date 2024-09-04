import os
import json
import requests
import jwt
import time

CLMS_URL = "https://land.copernicus.eu"


def get_clms_token():
    # Load saved key from filesystem

    service_key_file = os.environ.get("CLMS_KEY_FILE", "clms-key.json")
    service_key = json.load(open(service_key_file, "rb"))

    private_key = service_key["private_key"].encode("utf-8")

    claim_set = {
        "iss": service_key["client_id"],
        "sub": service_key["user_id"],
        "aud": service_key["token_uri"],
        "iat": int(time.time()),
        "exp": int(time.time() + (60 * 60)),
    }
    grant = jwt.encode(claim_set, private_key, algorithm="RS256")

    resp = requests.post(
        f"{CLMS_URL}/@@oauth2-token",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": grant,
        },
    )

    return resp.json().get("access_token", None)
