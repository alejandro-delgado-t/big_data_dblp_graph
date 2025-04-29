# big_data_dblp_graph
# Neo4j DBLP Project

## Overview

This project explores the DBLP dataset using Neo4j. It is divided into three main problem sections (A, B, C), each with its own explanation and Cypher code.

## Project Structure


- `ne04j_config/`  
  Backup of Neo4j settings (optional).  
  **Requirements to run the project:**
  - APOC library installed
  - Graph Data Science (GDS) library installed
  - Sufficient RAM limits set in Neo4j

- `Problem_A/`, `Problem_B/`, `Problem_C/`  
  Each folder contains:
  - A PDF file with explanations
  - The corresponding Cypher code for that part

- `data_clean/`  
  Contains three cleaned CSV files generated from the DBLP XML dataset using the `DBLP_processing_script/`.  
  > **Note:** The original DBLP XML file (~4GB) is not included.

- `DBLP_processing_script/`  
  Python script used to preprocess the raw DBLP XML and produce the cleaned CSV files.

## ⚠️ Important Setup Steps

1. **Before running any Cypher code**, import the three CSV files from `data_clean/` into your Neo4j database.
2. Make sure APOC and GDS libraries are installed.
3. Ensure your Neo4j instance has enough memory allocated to handle the dataset.

---
