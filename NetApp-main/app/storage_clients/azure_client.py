import os

AZURE_SIM_DIR = "/data/azure_blob_sim"
os.makedirs(AZURE_SIM_DIR, exist_ok=True)

class AzureClient:
    def __init__(self):
        self.base = AZURE_SIM_DIR

    def ensure_container(self, container: str):
        os.makedirs(os.path.join(self.base, container), exist_ok=True)

    def put_blob(self, container: str, name: str, data: bytes):
        path = os.path.join(self.base, container, name)
        with open(path, "wb") as f:
            f.write(data)

    def get_blob(self, container: str, name: str) -> bytes | None:
        path = os.path.join(self.base, container, name)
        try:
            with open(path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def delete_blob(self, container: str, name: str):
        path = os.path.join(self.base, container, name)
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
