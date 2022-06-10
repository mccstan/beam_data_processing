# python_data_pipeline
A python data pipeline for data engineering

## I.Python et Data Engineering

Le code source du projet est localisé sous `src`, les tests sous `test` les données sous `data`.
* `src/drugs_pipeline.py` : Pipeline de calcul du graphe de dépendance.
* `src/drugs_analytics.py` : Traitement ad-hoc pour calculer le journal avec le plus de références de médicaments.
* `src/utils/*.py` : Routines de traitements utilisés dans les pipelines.
* `test/*.py` : Des tests qui valident les pipelines developées.

Plusieurs modélisations étaient envisageables pour le graphe de dépendance.
Nous avons choisi un modèle plat basé sur les drugs et leur références.
Chaque ligne représente un `drug`, sa référencce éventuelle vers une publication de type `pubmed` ou `clinical trial`,
les journaux dans lesquels cette publication est apparue ainsi que la date de publication.
Il a fallut faire quelques réajustement :
    * Unification des colonnes `scientific_title`(clinical_trials) et `title`(pubmed)
    * Ajout d'une nouvelle colonne `type` avec les valeurs `pubmed` and `clinical trials`

Pourquoi ce choix de modélisation ?
* La simplicité : Notre modélisation est simple à cerner et permet rapidement de jouer avec la données et produire des insights.
* La performance : Il y'a toujours des compromis dans les choix de modélisation.
Cela dit, on choisi la modélisation qui permet d'optimisier les requêtes/traitements qui se feront sur la base de cette modélisation.
Dans le cas présent, le traitement demandé dans la partie ad-hoc s'y prête bien. Cette modélisation permet de répondre à la plus part des cas d'utilisation.

### Data pipeline

Résultat de la pipeline basé sur les échantillons fournis

---
|atccode|drug           |id         |title                                                                                                                                                                 |journal                                                    |type           |reference_date          |
|-------|---------------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|---------------|------------------------|
|A04AD  |DIPHENHYDRAMINE|1          |A 44-year-old man with erythema of the face diphenhydramine, neck, and chest, weakness, and palpitations                                                              |Journal of emergency nursing                               |pubmed         |2019-01-01T00:00:00.000Z|
|A04AD  |DIPHENHYDRAMINE|2          |An evaluation of benadryl, pyribenzamine, and other so-called diphenhydramine antihistaminic drugs in the treatment of allergy.                                       |Journal of emergency nursing                               |pubmed         |2019-01-01T00:00:00.000Z|
|A04AD  |DIPHENHYDRAMINE|3          |Diphenhydramine hydrochloride helps symptoms of ciguatera fish poisoning.                                                                                             |The Journal of pediatrics                                  |pubmed         |2019-01-02T00:00:00.000Z|
|S03AA  |TETRACYCLINE   |4          |Tetracycline Resistance Patterns of Lactobacillus buchneri Group Strains.                                                                                             |Journal of food protection                                 |pubmed         |2020-01-01T00:00:00.000Z|
|S03AA  |TETRACYCLINE   |5          |Appositional Tetracycline bone formation rates in the Beagle.                                                                                                         |American journal of veterinary research                    |pubmed         |2020-01-02T00:00:00.000Z|
|S03AA  |TETRACYCLINE   |6          |Rapid reacquisition of contextual fear following extinction in mice: effects of amount of extinction, tetracycline acute ethanol withdrawal, and ethanol intoxication.|Psychopharmacology                                         |pubmed         |2020-01-01T00:00:00.000Z|
|V03AB  |ETHANOL        |6          |Rapid reacquisition of contextual fear following extinction in mice: effects of amount of extinction, tetracycline acute ethanol withdrawal, and ethanol intoxication.|Psychopharmacology                                         |pubmed         |2020-01-01T00:00:00.000Z|
|A03BA  |ATROPINE       |           |Comparison of pressure BETAMETHASONE release, phonophoresis and dry needling in treatment of latent myofascial trigger point of upper trapezius ATROPINE muscle.      |The journal of maternal-fetal & neonatal medicine          |pubmed         |2020-03-01T00:00:00.000Z|
|A01AD  |EPINEPHRINE    |7          |The High Cost of Epinephrine Autoinjectors and Possible Alternatives.                                                                                                 |The journal of allergy and clinical immunology. In practice|pubmed         |2020-02-01T00:00:00.000Z|
|A01AD  |EPINEPHRINE    |8          |Time to epinephrine treatment is associated with the risk of mortality in children who achieve sustained ROSC after traumatic out-of-hospital cardiac arrest.         |The journal of allergy and clinical immunology. In practice|pubmed         |2020-03-01T00:00:00.000Z|
|6302001|ISOPRENALINE   |9          |Gold nanoparticles synthesized from Euphorbia fischeriana root by green route method alleviates the isoprenaline hydrochloride induced myocardial infarction in rats. |Journal of photochemistry and photobiology. B, Biology     |pubmed         |2020-01-01T00:00:00.000Z|
|R01AD  |BETAMETHASONE  |10         |Clinical implications of umbilical artery Doppler changes after betamethasone administration                                                                          |The journal of maternal-fetal & neonatal medicine          |pubmed         |2020-01-01T00:00:00.000Z|
|R01AD  |BETAMETHASONE  |11         |Effects of Topical Application of Betamethasone on Imiquimod-induced Psoriasis-like Skin Inflammation in Mice.                                                        |Journal of back and musculoskeletal rehabilitation         |pubmed         |2020-01-01T00:00:00.000Z|
|R01AD  |BETAMETHASONE  |           |Comparison of pressure BETAMETHASONE release, phonophoresis and dry needling in treatment of latent myofascial trigger point of upper trapezius ATROPINE muscle.      |The journal of maternal-fetal & neonatal medicine          |pubmed         |2020-03-01T00:00:00.000Z|
|A04AD  |DIPHENHYDRAMINE|NCT01967433|Use of Diphenhydramine as an Adjunctive Sedative for Colonoscopy in Patients Chronically on Opioids                                                                   |Journal of emergency nursing                               |clinical trials|2020-01-01T00:00:00.000Z|
|A04AD  |DIPHENHYDRAMINE|NCT04189588|Phase 2 Study IV QUZYTTIR™ (Cetirizine Hydrochloride Injection) vs V Diphenhydramine                                                                                  |Journal of emergency nursing                               |clinical trials|2020-01-01T00:00:00.000Z|
|A04AD  |DIPHENHYDRAMINE|NCT04237091|Feasibility of a Randomized Controlled Clinical Trial Comparing the Use of Cetirizine to Replace Diphenhydramine in the Prevention of Reactions Related to Paclitaxel |Journal of emergency nursing                               |clinical trials|2020-01-01T00:00:00.000Z|
|S03AA  |TETRACYCLINE   |           |                                                                                                                                                                      |                                                           |clinical trials|                        |
|V03AB  |ETHANOL        |           |                                                                                                                                                                      |                                                           |clinical trials|                        |
|A03BA  |ATROPINE       |           |                                                                                                                                                                      |                                                           |clinical trials|                        |
|A01AD  |EPINEPHRINE    |NCT04188184|Tranexamic Acid Versus Epinephrine During Exploratory Tympanotomy                                                                                                     |Journal of emergency nursing\xc3\x28                       |clinical trials|2020-04-27T00:00:00.000Z|
|6302001|ISOPRENALINE   |           |                                                                                                                                                                      |                                                           |clinical trials|                        |
|R01AD  |BETAMETHASONE  |NCT04153396|Preemptive Infiltration With Betamethasone and Ropivacaine for Postoperative Pain in Laminoplasty or \xc3\xb1 Laminectomy                                             |Hôpitaux Universitaires de Genève                          |clinical trials|2020-01-01T00:00:00.000Z|
---


Fichier Json produit : `data/processed/drug_references.json`

```json
[
{"atccode": "A04AD", "drug": "DIPHENHYDRAMINE", "title": "A 44-year-old man with erythema of the face diphenhydramine, neck, and chest, weakness, and palpitations", "journal": "Journal of emergency nursing", "date": "2019-01-01T00:00:00", "type": "pubmed", "source_id": "1"},
{"atccode": "A04AD", "drug": "DIPHENHYDRAMINE", "title": "An evaluation of benadryl, pyribenzamine, and other so-called diphenhydramine antihistaminic drugs in the treatment of allergy.", "journal": "Journal of emergency nursing", "date": "2019-01-01T00:00:00", "type": "pubmed", "source_id": "2"},
{"atccode": "A04AD", "drug": "DIPHENHYDRAMINE", "title": "Diphenhydramine hydrochloride helps symptoms of ciguatera fish poisoning.", "journal": "The Journal of pediatrics", "date": "2019-02-01T00:00:00", "type": "pubmed", "source_id": "3"},
{"atccode": "S03AA", "drug": "TETRACYCLINE", "title": "Tetracycline Resistance Patterns of Lactobacillus buchneri Group Strains.", "journal": "Journal of food protection", "date": "2020-01-01T00:00:00", "type": "pubmed", "source_id": "4"},
{"atccode": "S03AA", "drug": "TETRACYCLINE", "title": "Appositional Tetracycline bone formation rates in the Beagle.", "journal": "American journal of veterinary research", "date": "2020-02-01T00:00:00", "type": "pubmed", "source_id": "5"},
{"atccode": "S03AA", "drug": "TETRACYCLINE", "title": "Rapid reacquisition of contextual fear following extinction in mice: effects of amount of extinction, tetracycline acute ethanol withdrawal, and ethanol intoxication.", "journal": "Psychopharmacology", "date": "2020-01-01T00:00:00", "type": "pubmed", "source_id": "6"},
{"atccode": "V03AB", "drug": "ETHANOL", "title": "Rapid reacquisition of contextual fear following extinction in mice: effects of amount of extinction, tetracycline acute ethanol withdrawal, and ethanol intoxication.", "journal": "Psychopharmacology", "date": "2020-01-01T00:00:00", "type": "pubmed", "source_id": "6"},
{"atccode": "A03BA", "drug": "ATROPINE", "title": "Comparison of pressure BETAMETHASONE release, phonophoresis and dry needling in treatment of latent myofascial trigger point of upper trapezius ATROPINE muscle.", "journal": "The journal of maternal-fetal & neonatal medicine", "date": "2020-01-03T00:00:00", "type": "pubmed", "source_id": ""},
{"atccode": "A01AD", "drug": "EPINEPHRINE", "title": "The High Cost of Epinephrine Autoinjectors and Possible Alternatives.", "journal": "The journal of allergy and clinical immunology. In practice", "date": "2020-01-02T00:00:00", "type": "pubmed", "source_id": "7"},
{"atccode": "A01AD", "drug": "EPINEPHRINE", "title": "Time to epinephrine treatment is associated with the risk of mortality in children who achieve sustained ROSC after traumatic out-of-hospital cardiac arrest.", "journal": "The journal of allergy and clinical immunology. In practice", "date": "2020-01-03T00:00:00", "type": "pubmed", "source_id": "8"},
{"atccode": "6302001", "drug": "ISOPRENALINE", "title": "Gold nanoparticles synthesized from Euphorbia fischeriana root by green route method alleviates the isoprenaline hydrochloride induced myocardial infarction in rats.", "journal": "Journal of photochemistry and photobiology. B, Biology", "date": "2020-01-01T00:00:00", "type": "pubmed", "source_id": "9"},
{"atccode": "R01AD", "drug": "BETAMETHASONE", "title": "Clinical implications of umbilical artery Doppler changes after betamethasone administration", "journal": "The journal of maternal-fetal & neonatal medicine", "date": "2020-01-01T00:00:00", "type": "pubmed", "source_id": "10"},
{"atccode": "R01AD", "drug": "BETAMETHASONE", "title": "Effects of Topical Application of Betamethasone on Imiquimod-induced Psoriasis-like Skin Inflammation in Mice.", "journal": "Journal of back and musculoskeletal rehabilitation", "date": "2020-01-01T00:00:00", "type": "pubmed", "source_id": "11"},
{"atccode": "R01AD", "drug": "BETAMETHASONE", "title": "Comparison of pressure BETAMETHASONE release, phonophoresis and dry needling in treatment of latent myofascial trigger point of upper trapezius ATROPINE muscle.", "journal": "The journal of maternal-fetal & neonatal medicine", "date": "2020-01-03T00:00:00", "type": "pubmed", "source_id": ""},
{"atccode": "A04AD", "drug": "DIPHENHYDRAMINE", "title": "Use of Diphenhydramine as an Adjunctive Sedative for Colonoscopy in Patients Chronically on Opioids", "journal": "Journal of emergency nursing", "date": "2020-01-01T00:00:00", "type": "clinical trial", "source_id": "NCT01967433"},
{"atccode": "A04AD", "drug": "DIPHENHYDRAMINE", "title": "Phase 2 Study IV QUZYTTIR\u2122 (Cetirizine Hydrochloride Injection) vs V Diphenhydramine", "journal": "Journal of emergency nursing", "date": "2020-01-01T00:00:00", "type": "clinical trial", "source_id": "NCT04189588"},
{"atccode": "A04AD", "drug": "DIPHENHYDRAMINE", "title": "Feasibility of a Randomized Controlled Clinical Trial Comparing the Use of Cetirizine to Replace Diphenhydramine in the Prevention of Reactions Related to Paclitaxel", "journal": "Journal of emergency nursing", "date": "2020-01-01T00:00:00", "type": "clinical trial", "source_id": "NCT04237091"},
{"atccode": "A01AD", "drug": "EPINEPHRINE", "title": "Tranexamic Acid Versus Epinephrine During Exploratory Tympanotomy", "journal": "Journal of emergency nursing\\xc3\\x28", "date": "2020-04-27T00:00:00", "type": "clinical trial", "source_id": "NCT04188184"},
{"atccode": "R01AD", "drug": "BETAMETHASONE", "title": "Preemptive Infiltration With Betamethasone and Ropivacaine for Postoperative Pain in Laminoplasty or \\xc3\\xb1 Laminectomy", "journal": "H\u00f4pitaux Universitaires de Gen\u00e8ve", "date": "2020-01-01T00:00:00", "type": "clinical trial", "source_id": "NCT04153396"},
]

```

### Traitement ad-hoc

Résultat obtenu sur la base du sample fourni :`data/processed/top_journals_mentions.json`
```json
[
  {"journal": "The journal of maternal-fetal & neonatal medicine", "drugs_mentions": 2}
]

```
### Pour aller plus loin
Les choix de librairie, de framework et de design de pipeline ont été fait pour garantir :
* La simpliccité
* La réutilisabilité et la maintenabilité
* La scalabilité

Si il fallait gérer des fichiers de plusieurs To ou des millions de fichiers par exemple, les évolutions seraient mineurs.
En effet, nous avons choisi `Apache Beam` comme modèle de programmation.
Apache Beam est un modèle unifié Open Source permettant de définir des pipelines de traitement parallèle des données par lots et en streaming.
Le modèle de programmation Apache Beam simplifie la mécanique de traitement de données à grande échelle.

Ainsi le code présent permet aussi bien de traiter des workloads simples et léger mais aussi des workloads complexes avec des volumétries importantes.
Les adaptations se feront au niveau de l'infrastructure sur laquelle ce code est exécuté (Taille du cluster, CPU alloué, RAM disponible)

Par conséquent avec le présent code, il faut choisir l'un des backends de traitement distribué compatibles d'Apache Beam, tel que Dataflow, pour exécuter la pipeline.
Dans la pipeline actuelle on utilise par defaut le DirectRunner qui est un runner local. Il faudrait juste basculer le runner et définir les options necessaires pour le runner choisi.

## II. SQL

### DDL
Les requêtes SQL dans la suite ont été écrites pour fonctionner dans le moteur de base de données relationelles MySQL(MySQL v5.7).
Elles sont pensées avec simplicité pour s'adapter à la plus part des moteurs d'exécution avec des adaptations.
Ces requêtes peuvent être testées par ici https://www.db-fiddle.com/ en utilisant le DDL suivant.

```sql
CREATE TABLE product_nomenclature (
    product_id bigint PRIMARY KEY,
    product_type VARCHAR(30),
    product_name VARCHAR(30)
);

CREATE TABLE transactions (
    date date,
    order_id bigint,
    client_id bigint,
    prod_id bigint,
    prod_price DECIMAL(10, 2),
    prod_qty bigint,
    FOREIGN KEY (prod_id) REFERENCES product_nomenclature(product_id)
    ON DELETE SET NULL  ON UPDATE CASCADE
);

INSERT INTO product_nomenclature VALUES(490756, "MEUBLE", "Chaise");
INSERT INTO product_nomenclature VALUES(389728, "DECO", "Boule de Noël");
INSERT INTO product_nomenclature VALUES(549380, "MEUBLE", "Canapé");
INSERT INTO product_nomenclature VALUES(293718, "DECO", "Mug");

INSERT INTO transactions VALUES("20/01/01", 1234, 999, 490756, 50, 1);
INSERT INTO transactions VALUES("20/01/01", 1234, 999, 389728, 3.56, 4);
INSERT INTO transactions VALUES("20/01/01", 3456, 845, 490756, 50, 2);
INSERT INTO transactions VALUES("20/01/01", 3456, 845, 549380, 00, 1);
INSERT INTO transactions VALUES("20/01/01", 3456, 845, 293718, 10, 1);
INSERT INTO transactions VALUES("19/01/03", 3457, 846, 490756, 134, 6);
INSERT INTO transactions VALUES("19/02/08", 3458, 847, 389728, 100, 7);
INSERT INTO transactions VALUES("19/05/03", 3459, 848, 490756, 10, 0);
INSERT INTO transactions VALUES("19/12/31", 3460, 849, 549380, 100, 5);
```

### Le chiffre d’affaires (le montant total des ventes), jour par jour, du 1er janvier 2019 au 31 décembre 2019. Le résultat sera trié sur la date à laquelle la commande a été passée.

```sql
SELECT date, SUM(prod_price) AS ventes FROM transactions
    WHERE date BETWEEN '19/01/01' AND '19/12/31'
    GROUP BY date
```

Exemple de résultat sur la base de l'échantillon dans la partie DDL

---
| date       | ventes |
| ---------- | ------ |
| 2019-01-01 | 10.00  |
| 2019-01-03 | 134.00 |
| 2019-02-08 | 100.00 |
| 2019-05-03 | 10.00  |
| 2019-12-31 | 100.00 |
---

### Les ventes meuble et déco réaliséesles ventes meuble et déco réalisées par client et sur la période allant du 1er janvier 2020 au 31 décembre 2020.
```sql
SELECT clients.client_id, COALESCE(ventes_meubles, 0) AS ventes_meuble, COALESCE(ventes_deco, 0) AS ventes_deco FROM
    (SELECT distinct client_id FROM transactions) AS clients

    LEFT OUTER JOIN

    (SELECT client_id, SUM(prod_price) AS ventes_meubles FROM transactions, product_nomenclature
    WHERE prod_id = product_id AND product_type = "MEUBLE" AND date BETWEEN '20/01/01' AND '20/12/31'
    GROUP BY client_id) AS ventes_meubles_table
    ON clients.client_id = ventes_meubles_table.client_id

    LEFT OUTER JOIN

    (SELECT client_id, SUM(prod_price) AS ventes_deco FROM transactions, product_nomenclature
    WHERE prod_id = product_id AND product_type = "DECO" AND date BETWEEN '20/01/01' AND '20/12/31'
    GROUP BY client_id) AS ventes_deco_table
    ON clients.client_id = ventes_deco_table.client_id;
```

Exemple de résultat sur la base de l'échantillon dans la partie DDL

---
| client_id | ventes_meuble | ventes_deco |
| --------- | ------------- | ----------- |
| 990       | 0.00          | 979.12      |
| 999       | 50.00         | 36.56       |
| 845       | 50.00         | 0.00        |
| 846       | 0.00          | 0.00        |
| 847       | 0.00          | 0.00        |
| 848       | 0.00          | 0.00        |
| 849       | 0.00          | 0.00        |
---
