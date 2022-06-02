import time
import datetime
import os
import requests
import json
import pandas as pd
from project.server.main.logger import get_logger
from project.server.main.orcid import get_links_from_orcid

logger = get_logger(__name__)

def create_task_harvest(arg):
    cmd = "cat * | jq -r .publi_id | grep doi | cut -c 4- | sort -u"
