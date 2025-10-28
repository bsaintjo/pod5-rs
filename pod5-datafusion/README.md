# Integration of pod5 with Apache DataFusion

## Quick Start

```rust
```

## What is DataFusion?

## Why integrate POD5 with DataFusion?

- DataFusion uses Apache Arrow as its memory-model, simplifying integration.
- DataFusion is an extensible query engine. So one use case I'm thinking of, is that you could use this crate to take a list POD5s (like  a directory, or maybe AWS S3 instance) and add a database interface and query data from the files using SQL.
- Potential integration with other DataFusion projects, mainly thinking about `exon`[https://github.com/wheretrue/exon].
  - Analyses such as nucleotide modification detection with nanopore sequencing may need to integrate some combination of signal data (POD5), (un)aligned sequencing data (FASTA/Q, BAM), and annotations (GFF), and these datasets may be is different locations (local vs. object store).

## Mapping DataFusion concepts to the POD5 File format

- TableProvider: A list of Arrow RecordBatches = POD5 Signal/RunInfo/ReadsTable
- SchemaProvider: A list of TableProviders = a single POD5 file
- CatalogProvider: A list of schema providers = example would be a directory of POD5 files, but could be something else like an AWS S3 containing a bunch of POD5s, etc.
- CatalogProviderList: A list of catalog providers = Can be other databases that support DataFusion, similar to `exon`
