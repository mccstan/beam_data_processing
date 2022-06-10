import typing


class DrugReference(typing.NamedTuple):
    atccode: str
    drug: str
    title: str
    journal: str
    date: str
    type: str
    source_id: str
