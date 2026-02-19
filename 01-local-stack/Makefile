.PHONY: 
	all generate ingest test

# Run full pipeline from scratch
all: 
	generate init ingest test

# Generate synthetic data
generate:
	python -m src.generator.runner

# Reset database schemas  
init:
	python -m src.utils.init_db

# Ingest parquet to postgres
ingest:
	python -m src.ingestion.ingest

# Run validation
test:
	python -m src.generator.test

dbt-run:
	cd saas_sim && dbt run

dbt-test:
	cd saas_sim && dbt test

dbt-debug:
	cd saas_sim && dbt debug

reset:
	rm -rf data/raw/*