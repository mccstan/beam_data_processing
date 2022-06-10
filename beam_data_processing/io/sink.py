import json

import apache_beam as beam

from beam_data_processing.models.enriched import DrugReference
from beam_data_processing.models.raw import ClinicalTrial, Drug, Pubmed


@beam.ptransform_fn
@beam.typehints.with_input_types(beam.pvalue.PBegin)
def WriteJsonLines(pbegin: beam.pvalue.PBegin, filename: str, unique_file: bool = False):
    def dump(record):
        if isinstance(record, (DrugReference, Drug, Pubmed, ClinicalTrial)):
            dumped = json.dumps(record._asdict())
        else:
            dumped = json.dumps(record)
        return dumped

    def dump_record(elem) -> str:
        if unique_file:
            return f"{dump(elem)},"
        else:
            dump(elem)

    shard_name_template = "" if unique_file else "-SSSSS-of-NNNNN"
    header = "[" if unique_file else ""
    footer = "]" if unique_file else ""

    (
        pbegin
        | "Serialize" >> beam.Map(dump_record)
        | "Write unique json file"
        >> beam.io.WriteToText(
            file_path_prefix=filename, shard_name_template=shard_name_template, header=header, footer=footer
        )
    )
