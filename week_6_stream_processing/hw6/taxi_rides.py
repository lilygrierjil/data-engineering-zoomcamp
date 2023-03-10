import faust
from typing import Optional

class TaxiRide(faust.Record, validation=True):
    vendorId: str
    PULocationID: Optional[str]
    DOLocationID: Optional[str]