import argparse
import logging

import apache_beam
import apache_beam as beam
from apache_beam import RowCoder
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from beam_data_processing.io.sink import WriteJsonLines
from beam_data_processing.io.source import ReadCsvFiles, ReadMultiLinesJsonFiles
from beam_data_processing.models.enriched import DrugReference
from beam_data_processing.models.raw import ClinicalTrial, Drug, Pubmed
from beam_data_processing.transform.drugs_processing import ProcessDrugReferences


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

    pubmed_csv_path = "./data/raw/pubmed.csv"
    pubmed_json_path = "./data/raw/pubmed.json"
    drugs_csv_path = "./data/raw/drugs.csv"
    clinical_trials_csv_path = "./data/raw/clinical_trials.csv"
    drugs_references_path = "./data/processed/drug_references.json"

    apache_beam.coders.registry.register_coder(Pubmed, RowCoder)
    apache_beam.coders.registry.register_coder(Drug, RowCoder)
    apache_beam.coders.registry.register_coder(ClinicalTrial, RowCoder)
    apache_beam.coders.registry.register_coder(DrugReference, RowCoder)

    with beam.Pipeline(options=pipeline_options) as p:
        pubmed_csv = p | "Load pubmed csv" >> ReadCsvFiles(pubmed_csv_path)
        pubmed_json = p | "Load pubmed json" >> ReadMultiLinesJsonFiles(pubmed_json_path)
        drugs_csv = p | "Load drugs data" >> ReadCsvFiles(drugs_csv_path)
        clinical_trials_csv = p | "Load clinical trials data" >> ReadCsvFiles(clinical_trials_csv_path)

        pubmed_data = (
            (pubmed_csv, pubmed_json)
            | "Merge pubmed data" >> beam.Flatten()
            | "Map pubmed to NamedTuple" >> beam.Map(lambda record: Pubmed(**record)).with_output_types(Pubmed)
        )
        drugs_data = drugs_csv | "Map drug to NamedTuple" >> beam.Map(lambda record: Drug(**record)).with_output_types(
            Drug
        )

        clinical_trials_data = clinical_trials_csv | "Map clinical_trial to NamedTuple" >> beam.Map(
            lambda record: ClinicalTrial(**record)
        ).with_output_types(ClinicalTrial)

        keyed_drug = drugs_data | "Keyed drug" >> beam.WithKeys(0)
        keyed_pubmed = pubmed_data | "Keyed pubmed" >> beam.WithKeys(0)
        keyed_clinical_trial = clinical_trials_data | "Keyed clinical_trial" >> beam.WithKeys(0)

        joined = (
            {
                "drug": keyed_drug,
                "pubmed": keyed_pubmed,
                "clinical_trial": keyed_clinical_trial,
            }
        ) | "Join data" >> beam.CoGroupByKey()

        drug_references = joined | ProcessDrugReferences()

        drug_references | WriteJsonLines(filename=drugs_references_path, unique_file=True)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
