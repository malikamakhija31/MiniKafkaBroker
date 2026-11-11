
import hashlib


class Partitioner:

    @staticmethod
    def get_partition(key: str, partition_count: int) -> int:

        if partition_count <= 0:
            raise ValueError("Partition count must be positive")

        key_bytes = key.encode("utf-8")

        hash_value = hashlib.sha256(key_bytes).digest()

        number = int.from_bytes(hash_value, byteorder="big")

        return number % partition_count
