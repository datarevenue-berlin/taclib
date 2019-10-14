#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `taclib` package."""

import pytest
from taclib.config import config


def test_config():
    config["image"].get(str) == "python3:latest"
