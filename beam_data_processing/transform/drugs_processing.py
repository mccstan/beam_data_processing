import itertools
from typing import Dict, List, Tuple, Union

import apache_beam as beam
from dateutil import parser

from beam_data_processing.models.enriched import DrugReference
from beam_data_processing.models.raw import ClinicalTrial, Drug, Pubmed


@beam.ptransform_fn
@beam.typehints.with_input_types(beam.pvalue.PBegin)
@beam.typehints.with_output_types(DrugReference)
def ProcessDrugReferences(pbegin: beam.pvalue.PBegin) -> beam.PCollection[DrugReference]:
    def build_references(elem: Tuple[int, Dict[str, Union[List[ClinicalTrial], List[Drug], List[Pubmed]]]]):
        group_key, group = elem
        drug = group.get("drug")
        pubmed = group.get("pubmed")
        clinical_trial = group.get("clinical_trial")

        for raw_joined in itertools.product(drug, pubmed):
            joined_drug, joined_pubmed = raw_joined
            if joined_drug.drug.lower() in joined_pubmed.title.lower():
                yield DrugReference(
                    atccode=joined_drug.atccode,
                    drug=joined_drug.drug,
                    title=joined_pubmed.title,
                    date=parser.parse(joined_pubmed.date).isoformat(),
                    journal=joined_pubmed.journal,
                    type="pubmed",
                    source_id=joined_pubmed.id,
                )

        for raw_joined in itertools.product(drug, clinical_trial):
            joined_drug, joined_clinical_trial = raw_joined
            if joined_drug.drug.lower() in joined_clinical_trial.scientific_title.lower():
                yield DrugReference(
                    atccode=joined_drug.atccode,
                    drug=joined_drug.drug,
                    title=joined_clinical_trial.scientific_title,
                    date=parser.parse(joined_clinical_trial.date).isoformat(),
                    journal=joined_clinical_trial.journal,
                    type="clinical trial",
                    source_id=joined_clinical_trial.id,
                )

    return pbegin | "Process Drug References" >> beam.FlatMap(build_references)
