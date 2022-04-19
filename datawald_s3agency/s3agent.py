#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

from .s3agency import S3Agency


class S3Agent(S3Agency):
    def __init__(self, logger, **setting):
        S3Agency.__init__(self, logger, **setting)
