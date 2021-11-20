from __future__ import annotations

import os
from typing import Any

import boto3
from logzero import logger


DDB_RESOURCE = None
STATS_TABLE_NAME = None
STATS_TABLE = None
TABLE_PKEY = "stat"

if os.environ.get("ENV", "local").lower() != "local":
    DDB_RESOURCE = boto3.resource("dynamodb")
    STATS_TABLE_NAME = os.environ.get("STATS_TABLE")
    STATS_TABLE = DDB_RESOURCE.Table(STATS_TABLE_NAME)


def get_item(key: Any) -> dict:
    result = STATS_TABLE.get_item(Key={TABLE_PKEY: key})
    logger.info(f"DDB.GetItem response: {result}")
    return result.get("Item")


def update_item(key: Any, attr: dict) -> bool:
    attr.pop(TABLE_PKEY, None)
    update_exp, ean, eav = make_update_item_assets(attr)
    result = STATS_TABLE.update_item(
        Key={TABLE_PKEY: key},
        UpdateExpression=update_exp,
        ExpressionAttributeNames=ean,
        ExpressionAttributeValues=eav,
    )
    logger.info(f"DDB.UpdateItem response: {result}")


# UpdateItem requires explicit setting of attributes
# No need to pass back iteration history
# Iteration on a dict is insertion order based
# Returns of tuple of update expression, EAN, and EAV
# EAN = Expression Attribute Names
# EAV = Expression Attribute Values
def make_update_item_assets(attributes: dict) -> tuple[str, dict, dict]:
    if len(attributes) == 0:
        logger.warn("UpdateItem was passed an empty attribute dict")
        return "", {}, {}

    counter = 1
    update_exp_frags = []
    ean = {}
    eav = {}
    for key in attributes:
        if type(key) != str:
            continue

        update_exp_frags.append(f"#{counter} = :{counter}")
        ean[f"#{counter}"] = key
        eav[f":{counter}"] = attributes[key]
        counter += 1

    update_exp = ", ".join(update_exp_frags)
    return (f"SET {update_exp}", ean, eav)
