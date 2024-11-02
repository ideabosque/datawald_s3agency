#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

from .s3agency import S3Agency


def deploy() -> list:
    return [
        {
            "service": "DataWald",
            "class": "S3Agent",
            "functions": {
                "stream_handle": {
                    "is_static": False,
                    "label": "s3agency",
                    "mutation": [],
                    "query": [],
                    "type": "Event",
                    "support_methods": [],
                    "is_auth_required": False,
                    "is_graphql": False,
                    "settings": "datawald_agency",
                    "disabled_in_resources": True,  # Ignore adding to resource list.
                },
                "insert_update_entities_to_target": {
                    "is_static": False,
                    "label": "s3agency",
                    "mutation": [],
                    "query": [],
                    "type": "Event",
                    "support_methods": [],
                    "is_auth_required": False,
                    "is_graphql": False,
                    "settings": "datawald_agency",
                    "disabled_in_resources": True,  # Ignore adding to resource list.
                },
                "update_sync_task": {
                    "is_static": False,
                    "label": "s3agency",
                    "mutation": [],
                    "query": [],
                    "type": "Event",
                    "support_methods": [],
                    "is_auth_required": False,
                    "is_graphql": False,
                    "settings": "datawald_agency",
                    "disabled_in_resources": True,  # Ignore adding to resource list.
                },
            },
        }
    ]


class S3Agent(S3Agency):
    def __init__(self, logger, **setting):
        S3Agency.__init__(self, logger, **setting)
