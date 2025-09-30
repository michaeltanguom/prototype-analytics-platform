# Prototype Analytics Platform Architecture

## Overview

This document describes the architecture of the Prototype Analytics Platform, which is designed to ingest, process, and analyse bibliometric data from various sources.

The architecture follows the medallion pattern with three distinct layers:

1. **Bronze Layer**: Raw data ingestion and initial normalisation
2. **Silver Layer**: Data Vault 2.0 modelled data warehouse
3. **Gold Layer**: Business-specific aggregated datasets and metrics

## System Architecture

![Architecture Diagram](./img/architecture_diagram.png)

### Components

#### Data Sources
- **Scopus**: Publication metadata, citations, and author information
- **SciVal**: Metrics and benchmarking data
- **Web of Science**: Publication metadata and citation data
- **Pure**: Institutional research information
- **Overton**: Policy document citations and mentions
- **Altmetric**: Alternative metrics and social media mentions

#### Bronze Layer (Data Ingestion)
- **API Connectors**: Individual connectors for each data source
- **Raw Storage**: Year/month partitioned JSON files with complete API responses
- **Bronze Tables**: Normalised tables with standard schemas for each entity type
- **Metadata Management**: Tracking of data provenance, timestamps, and versions

#### Silver Layer (Data Vault 2.0)
- **Hubs**: Core business entities (publications, authors, institutions)
- **Links**: Relationships between entities
- **Satellites**: Descriptive and temporal attributes for entities
- **Reference Tables**: Static lookup values and classifications

#### Gold Layer (Business Models)
- **Dimensional Models**: Star schemas optimised for specific analytical use cases
- **Aggregated Metrics**: Pre-calculated metrics and KPIs
- **Materialised Views**: Optimised views for frequent queries

#### Utilities and Services
- **Orchestration**: Workflow management for data processing pipelines
- **Logging**: Comprehensive activity and error logging
- **Monitoring**: Performance and data quality monitoring
- **Configuration**: Centralised configuration management

## Data Flow

### Ingestion Flow
1. API connectors retrieve data from bibliometric sources
2. Raw responses are stored with metadata in partitioned directories
3. Data is extracted and normalised into bronze tables
4. Publication IDs are extracted for cross-referencing between sources

### Silver Layer Processing
1. Business keys are extracted from bronze tables
2. Core entities are loaded into hub tables
3. Relationships are identified and loaded into link tables
4. Descriptive attributes are loaded into satellite tables
5. Historical changes are tracked using effective dates

### Gold Layer Generation
1. Business-specific dimensional models are created from the Data Vault
2. Metrics are calculated and aggregated at appropriate levels
3. Materialised views are refreshed for performance optimisation

## Technologies

- **Storage**: DuckDB for local development, potential for PostgreSQL in production
- **Processing**: Python data processing frameworks
- **Orchestration**: Simple pipeline scripts, potential for Airflow in production
- **Version Control**: Git for code and configuration

## Data Vault 2.0 Model

The Silver layer implements a Data Vault 2.0 architecture with the following components:

### Hub Tables
- Hub_Publication
- Hub_Author
- Hub_Institution
- Hub_Journal
- Hub_Funder
- Hub_Policy

### Link Tables
- Link_Author_Publication
- Link_Institution_Author
- Link_Funder_Publication
- Link_Policy_Publication

### Satellite Tables
- Sat_Publication
- Sat_Publication_Metrics
- Sat_Author
- Sat_Institution
- Sat_Policy

## Security and Governance

- **API Keys**: Stored as environment variables, not in code
- **Access Control**: File system permissions for local development
- **Data Lineage**: Tracking of data sources and transformations
- **Compliance**: Adherence to data usage terms from providers

## Extensibility

The platform is designed to be extensible in several ways:

1. **New Data Sources**: Additional API connectors can be added to the bronze layer
2. **New Metrics**: Additional metrics can be calculated in the gold layer
3. **New Analyses**: New dimensional models can be added for specific analytical needs
4. **Scaling**: Architecture can scale from local development to cloud deployment
