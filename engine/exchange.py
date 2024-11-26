import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import random
from typing import List, Dict

from client.custom_formatter import LogFactory


class Exchange:
    """Class that initializes matching engines, synchronizers, and bootstraps
    client connections to matching engines.
    """

    def __init__(
        self,
        me_data: Dict[str, Dict[str, str]],
        authentication_key: str
    ):
        """
        me_data looks like {me_name: {location_x : <float>, location_y: <float>, address: <string>}}
        """
        self.name = "Exchange"
        self.me_data = me_data
        self.authentication_key = authentication_key

        # logging
        self.log_directory = os.getcwd() + "/logs/exchange_logs/"
        self.logger = LogFactory(self.name, self.log_directory).get_logger()

    def assign_client(self, client_x: float, client_y: float):
        """ Assigns clients to matching engines randomly """
        matched_me_name = random.choice(list(self.me_data.keys()))
        self.logger.info(f"assigned client to {matched_me_name}")
        return self.me_data[matched_me_name]["address"]

    def authenticate(self, client_id: str, client_authentication: str):
        # TODO: Actually check password
        self.logger.info(f"authenticated client {client_id}")

        return True
