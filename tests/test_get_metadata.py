from spellbook.metadata import Entity, Feature, Group, MetaData


def test_metadata_read():
    metadata_list = [
        Entity(name="user", value_type=str),
        Group(
            name="table",
            entity="user",
            features=[
                Feature(name="a", value_type=int),
                Feature(name="b", value_type=int),
                Feature(name="c", value_type=int),
            ],
        ),
    ]
    base_meta = MetaData(
        entities=[Entity(name="user", value_type=str)],
        groups=[
            Group(
                name="table",
                entity="user",
                features=[
                    Feature(name="a", value_type=int),
                    Feature(name="b", value_type=int),
                    Feature(name="c", value_type=int),
                ],
            )
        ],
    )

    assert base_meta == MetaData.parse_list(metadata_list)


def test_parse_yaml_simple():
    sample_yaml = """
---
kind: entity
name: user
value_type: str
    
    
"""
    print(MetaData.parse_yaml(sample_yaml))
    assert MetaData.parse_yaml(sample_yaml) == MetaData(entities=[Entity(name="user", value_type="str")], groups=[])
