from spellbook.base import Entity, Feature, Group, RepoConfig


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
    base_meta = RepoConfig(
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

    assert base_meta == RepoConfig.parse_list(metadata_list)


def test_parse_yaml_simple():
    sample_yaml = """
---
kind: entity
name: user
value_type: str

"""
    print(RepoConfig.parse_yaml(sample_yaml))
    assert RepoConfig.parse_yaml(sample_yaml) == RepoConfig(entities=[Entity(name="user", value_type="str")], groups=[])
