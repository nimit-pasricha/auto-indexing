import time
import json
import re
import os
from kafka import KafkaProducer

LOG_PATTERN = re.compile(
    r"statement: SELECT .* FROM ([\w\.]+) WHERE (\w+)\s*([<>=!]+)", 
    re.IGNORECASE
)
