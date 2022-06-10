import typing

import apache_beam
from apache_beam import RowCoder


class Drug(typing.NamedTuple):
    atccode: str
    drug: str


class Pubmed(typing.NamedTuple):
    id: str
    title: str
    date: str
    journal: str


class ClinicalTrial(typing.NamedTuple):
    id: str
    scientific_title: str
    date: str
    journal: str
