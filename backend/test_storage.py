
import asyncio

from app.models.record import Record
from app.storage.segment import Segment


async def main():

    segment = Segment("data/test/00000000.log")

    record = Record(
        topic="orders",
        partition=0,
        offset=0,
        key="order-101",
        value={
            "product": "Laptop",
            "amount": 50000
        }
    )

    result = await segment.append(record)

    print("Record successfully stored!")
    print("Storage details:", result)


asyncio.run(main())
