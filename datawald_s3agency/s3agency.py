#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import traceback
from datawald_agency import Agency
from datawald_connector import DatawaldConnector
from s3_connector import S3Connector
from datetime import datetime


class S3Agency(Agency):
    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.s3_connector = S3Connector(logger, **setting)
        self.datawald = DatawaldConnector(logger, **setting)
        Agency.__init__(self, logger, datawald=self.datawald)

        self.map = setting.get("TXMAP", {})

    def tx_assets_src(self, **kwargs):
        try:
            if kwargs.get("bucket") and kwargs.get("key"):
                bucket = kwargs.get("bucket")
                key = kwargs.get("key")
                raw_assets = self.s3_connector.get_rows(bucket, key)
            else:
                raw_assets = self.s3_connector.get_objects(
                    kwargs.get("tx_type"), **kwargs
                )

            if len(raw_assets) == 0:
                return raw_assets

            if kwargs.get("tx_type") not in ("product", "inventory"):
                raise Exception(f"{kwargs.get('tx_type')} is not supported.")

            if kwargs.get("tx_type") == "product":
                kwargs.update({"metadatas": self.get_product_metadatas(**kwargs)})

            assets = list(
                map(
                    lambda raw_asset: self.tx_asset_src(raw_asset, **kwargs),
                    raw_assets,
                )
            )
            return assets
        except Exception:
            self.logger.info(kwargs)
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def tx_asset_src(self, raw_asset, **kwargs):
        tx_type = kwargs.get("tx_type")
        target = kwargs.get("target")
        asset = {
            "src_id": raw_asset[self.setting["src_metadata"][tx_type]["src_id"]],
            "created_at": raw_asset[
                self.setting["src_metadata"][tx_type]["created_at"]
            ],
            "updated_at": raw_asset[
                self.setting["src_metadata"][tx_type]["updated_at"]
            ],
        }

        try:
            if tx_type == "product":
                metadatas = kwargs.get("metadatas")
                data = self.transform_data(raw_asset, metadatas)
                asset.update({"data": data})
            elif tx_type == "inventory":
                raw_asset.update(kwargs)
                if len([key for key in raw_asset.keys() if key.find("|") != -1]) > 0:
                    raw_asset.update(
                        {
                            "inventory": self.tx_inventory_src(
                                {
                                    k: v
                                    for k, v in raw_asset.items()
                                    if k.find("|") != -1
                                }
                            )
                        }
                    )

                asset.update(
                    {
                        "data": self.transform_data(raw_asset, self.map[target].get(tx_type)),
                    }
                )
            else:
                raise Exception(f"{tx_type} is not supported.")

        except Exception:
            log = traceback.format_exc()
            asset.update({"tx_status": "F", "tx_note": log})
            self.logger.exception(log)
        return asset

    def tx_inventory_src(self, raw_inventory):
        return [
            dict(
                {"warehouse": warehouse},
                **{
                    k.split("|")[1]: v
                    for k, v in raw_inventory.items()
                    if k.find(warehouse) != -1
                },
            )
            for warehouse in set([key.split("|")[0] for key in raw_inventory.keys()])
        ]
