"""
Handles and parses the metadata files
"""

from datetime import datetime
from enum import Enum
from typing import List, Literal, Optional, Union

import yaml
from pydantic import BaseModel, validator
from tabulate import tabulate
from yaml.parser import ParserError


class Entity(BaseModel):
    name: str
    value_type: Optional[type]  # probably need sqlalchemy types in future or a mapping
    description: Optional[str]
    kind: str = "entity"

    @validator("value_type", pre=True)
    def convert_str_type(cls, v):
        if type(v) is str:
            type_mapper = {"str": str, "int": int, "float": float, "datetime": datetime}
            return type_mapper[v]
        return v


class Feature(BaseModel):
    name: str
    value_type: type  # probably need sqlalchemy types in future or a mapping
    description: Optional[str]
    kind: str = "feature"

    @validator("value_type", pre=True)
    def convert_str_type(cls, v):
        if type(v) is str:
            type_mapper = {"str": str, "int": int, "float": float, "datetime": datetime}
            return type_mapper[v]
        return v


class Group(BaseModel):
    name: str
    entity: str
    features: List[Feature]
    description: Optional[str]
    event_timestamp_column: Optional[str]  # if not available, just just assume the feature group has no timestamp
    created_timestamp_column: Optional[str]  # if available, will order by created_timestamp
    kind: str = "group"


class MetaData(BaseModel):
    entities: List[Entity]
    groups: List[Group]

    @classmethod
    def parse_list(cls, list_obj):
        entities = []
        groups = []
        for obj in list_obj:
            if type(obj) is Entity:
                entities.append(obj)
            elif type(obj) is Group:
                groups.append(obj)
            else:
                raise ValueError(f"Expected Entity or Group object, got: {type(obj)}")
        return cls(entities=entities, groups=groups)

    @classmethod
    def parse_yaml(cls, config):
        meta_list_dict = yaml.safe_load_all(config)
        meta_list = []
        for meta_obj in meta_list_dict:
            if meta_obj["kind"] == "entity":
                meta_list.append(Entity.parse_obj(meta_obj))
            elif meta_obj["kind"] == "group":
                meta_list.append(Group.parse_obj(meta_obj))
            else:
                raise ParserError("Unable to parse Configuration...")
        return cls.parse_list(meta_list)

    @classmethod
    def parse_yaml_file(cls, config_file):
        config = open(config_file, "r").read()
        return cls.parse_yaml(config)

    def print_entity(self):
        headers = ["name", "value-type", "description"]
        table = [[e.name, str(e.value_type), e.description] for e in self.entities]
        return tabulate(table, headers, tablefmt="github")

    def print_group(self):
        headers = ["name", "entity", "description", "event_timestamp", "created_timestamp"]
        table = [
            [g.name, str(g.entity), g.description, g.event_timestamp_column, g.created_timestamp_column]
            for g in self.groups
        ]
        return tabulate(table, headers, tablefmt="github")

    def print_feature(self):
        headers = ["name", "group", "entity", "value-type", "description"]
        table = []
        for g in self.groups:
            for f in g.features:
                table.append([f.name, g.name, str(g.entity), str(f.value_type), f.description])
        return tabulate(table, headers, tablefmt="github")

    def print_meta(self, subset: Optional[Literal["group", "feature", "entity"]] = None):
        if subset is None:
            output = f"""
Entity
{self.print_entity()}

Group
{self.print_group()}

Feature
{self.print_feature()}
"""
            return output
        elif subset == "group":
            return f"\n{self.print_group()}\n"
        elif subset == "feature":
            return f"\n{self.print_feature()}\n"
        elif subset == "entity":
            return f"\n{self.print_entity()}"
        else:
            raise ValueError(f"Subset does not appear one of (group, feature, entity) - got: {subset}.")
