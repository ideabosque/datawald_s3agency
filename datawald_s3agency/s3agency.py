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
        if setting.get("tx_type"):
            Agency.tx_type = setting.get("tx_type")

        self.map = setting.get("TXMAP", {})

    def get_parser_name(self, **kwargs):
        id = kwargs.get(self.setting["parser_name"]["id"][kwargs["tx_type"]])
        return self.setting["parser_name"]["data"][id]

    def tx_entities_src(self, **kwargs):
        try:
            if kwargs.get("bucket") and kwargs.get("key"):
                if kwargs.get("key").find(".csv") != -1:
                    raw_entities = self.s3_connector.get_rows(
                        kwargs.get("bucket"), kwargs.get("key"), '\n'
                    )
                elif kwargs.get("key").find(".xlsx") != -1:
                    raw_entities = self.s3_connector.get_rows(
                        kwargs.get("bucket"), kwargs.get("key")
                    )
                elif kwargs.get("key").find(".pdf") != -1:
                    parser_name = self.get_parser_name(**kwargs)
                    raw_entities = self.s3_connector.get_data_by_docparser(
                        kwargs.get("bucket"),
                        kwargs.get("key"),
                        parser_name,
                    )
                else:
                    raise Exception(f"{kwargs.get('key')} is not supported.")
            else:
                raw_entities = self.s3_connector.get_objects(
                    kwargs.get("tx_type"), **kwargs
                )

            if len(raw_entities) == 0:
                return raw_entities

            if kwargs.get("tx_type") == "product":
                kwargs.update({"metadatas": self.get_product_metadatas(**kwargs)})

            entities = list(
                map(
                    lambda raw_entity: self.tx_entity_src(raw_entity, **kwargs),
                    raw_entities,
                )
            )
            return entities
        except Exception:
            self.logger.info(kwargs)
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def tx_entity_src(self, raw_entity, **kwargs):
        tx_type = kwargs.get("tx_type")
        target = kwargs.get("target")
        entity = {
            "src_id": raw_entity[self.setting["src_metadata"][tx_type]["src_id"]],
            "created_at": raw_entity[
                self.setting["src_metadata"][tx_type]["created_at"]
            ],
            "updated_at": raw_entity[
                self.setting["src_metadata"][tx_type]["updated_at"]
            ],
        }

        try:
            raw_entity.update(kwargs)
            if tx_type == "product":
                metadatas = kwargs.get("metadatas")
                data = self.transform_data(raw_entity, metadatas)
                entity.update({"data": data})
            else:
                entity.update(
                    {
                        "data": self.transform_data(
                            raw_entity, self.map[target].get(tx_type)
                        ),
                    }
                )

        except Exception:
            log = traceback.format_exc()
            entity.update({"tx_status": "F", "tx_note": log})
            self.logger.exception(log)
        return entity

    def tx_transactions_src(self, **kwargs):
        return self.tx_entities_src(**kwargs)

    def tx_persons_src(self, **kwargs):
        return self.tx_entities_src(**kwargs)

    def tx_assets_src(self, **kwargs):
        return self.tx_entities_src(**kwargs)
