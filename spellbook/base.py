"""
Handles and parses the metadata files
"""

import os
from datetime import datetime
from typing import List, Literal, Optional, Union

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, validator
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from tabulate import tabulate


class EngineConfig(object):
    def __init__(self, url: str, config: dict):
        self.url = url
        self.config = self.load_envvars(config)

    def _fix_envvars(self, nm):
        if os.path.exists(".env"):
            load_dotenv()
        if type(nm) is str:
            nm = nm.strip()
            if nm.startswith("${") and nm.endswith("}"):
                nm = nm[2:-1]
                nm = os.environ[nm]
        return nm

    def load_envvars(self, config):
        config = {k: self._fix_envvars(v) for k, v in config.items()}
        return config

    @classmethod
    def parse_obj(cls, v):
        ignore_keys = ["url", "kind"]
        url = v["url"]
        config = {k: v for k, v in v.items() if k not in ignore_keys}
        return cls(url, config)

    def get_engine(self):
        return create_engine(self.url, **self.config)


class Entity(BaseModel):
    name: str
    value_type: Optional[type]  # probably need sqlalchemy types in future or a mapping
    description: Optional[str]
    kind: str = "entity"

    @validator("value_type", pre=True, allow_reuse=True)
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

    @validator("value_type", pre=True, allow_reuse=True)
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
    create_timestamp_column: Optional[
        str
    ]  # if not available, just just assume the feature group has no create timestamp
    kind: str = "group"


class RepoConfig(BaseModel):
    entities: List[Entity]
    groups: List[Group]
    engine: Optional[Union[Engine, EngineConfig]]

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def parse_list(cls, list_obj):
        entities = []
        groups = []
        engine = None
        for obj in list_obj:
            if type(obj) is Entity:
                entities.append(obj)
            elif type(obj) is Group:
                groups.append(obj)
            elif type(obj) in [Engine]:
                engine = obj
            elif type(obj) is EngineConfig:
                engine = obj.get_engine()
            else:
                raise ValueError(f"Expected Entity or Group object, got: {type(obj)}")
        return cls(entities=entities, groups=groups, engine=engine)

    @classmethod
    def parse_yaml(cls, config):
        meta_list_dict = yaml.safe_load_all(config)
        meta_list = []
        for meta_obj in meta_list_dict:
            if meta_obj["kind"] == "entity":
                meta_list.append(Entity.parse_obj(meta_obj))
            elif meta_obj["kind"] == "group":
                meta_list.append(Group.parse_obj(meta_obj))
            elif meta_obj["kind"] == "engine":
                meta_list.append(EngineConfig.parse_obj(meta_obj))
            else:
                raise Exception("Unable to parse Configuration...")
        return cls.parse_list(meta_list)

    @classmethod
    def parse_yaml_file(cls, config_file):
        config = open(config_file, "r").read()
        return cls.parse_yaml(config)

    def get_attr_from_group_name(self, group_name, attr_name):
        for g in self.groups:
            if g.name == group_name:
                return getattr(g, attr_name)
        raise ValueError(f"Group name: {group_name}, not found in Repo Configuration!")

    def print_entity(self):
        headers = ["name", "value-type", "description"]
        table = [[e.name, str(e.value_type), e.description] for e in self.entities]
        return tabulate(table, headers, tablefmt="pipe")

    def print_group(self):
        headers = ["name", "entity", "description", "event_timestamp"]
        table = [[g.name, str(g.entity), g.description, g.event_timestamp_column] for g in self.groups]
        return tabulate(table, headers, tablefmt="pipe")

    def print_feature(self):
        headers = ["name", "group", "entity", "value-type", "description"]
        table = []
        for g in self.groups:
            for f in g.features:
                table.append([f.name, g.name, str(g.entity), str(f.value_type), f.description])
        return tabulate(table, headers, tablefmt="pipe")

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
