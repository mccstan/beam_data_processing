import argparse
import itertools
import logging

import apache_beam
import apache_beam as beam
from apache_beam import RowCoder
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from dateutil import parser

from beam_data_processing.io.sink import WriteJsonLines
from beam_data_processing.io.source import ReadCsvFiles, ReadMultiLinesJsonFiles
from beam_data_processing.models.enriched import DrugReference
from beam_data_processing.models.raw import ClinicalTrial, Drug, Pubmed


def main(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    argument_parser = argparse.ArgumentParser()

    known_args, pipeline_args = argument_parser.parse_known_args(argv)
    pipeline_args.extend(
        [
            "--runner=DirectRunner",
            "--project=TODO",
            "--region=TODO",
            "--temp_location=gs://TODO",
            "--job_name=drugs_pipeline",
        ]
    )

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    drugs_references_path = "./data/processed/drug_references.json"
    top_journals_mentions_path = "./data/processed/top_journals_mentions.json"

    apache_beam.coders.registry.register_coder(DrugReference, RowCoder)

    with beam.Pipeline(options=pipeline_options) as p:
        drugs_references_json = p | "Load Drug Reference Data" >> ReadMultiLinesJsonFiles(drugs_references_path)

        drugs_references_data = drugs_references_json | "Map drug to NamedTuple" >> beam.Map(
            lambda record: DrugReference(**record)
        ).with_output_types(DrugReference)

        journals_and_drugs = (
            drugs_references_data
            | "Map to Key value : (journal, drug)" >> beam.Map(lambda record: (record.journal, record.drug))
            | "Deduplicate elements" >> beam.Distinct()
        )

        journals_mentions_count = (
            journals_and_drugs | beam.GroupByKey() | beam.Map(lambda record: (record[0], len(record[1])))
        )

        top_mentions_journal = (
            journals_mentions_count
            | beam.CombineGlobally(lambda records: max([elem for elem in records] or [None]))
            | beam.Map(lambda record: {"journal": record[0], "drugs_mentions": record[1]})
        )

        top_mentions_journal | WriteJsonLines(filename=top_journals_mentions_path, unique_file=True)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
