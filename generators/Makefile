gen:
	python ./adp_generator.py --action generate_all_objects --database ALLIANT_PPL_PROD --schema DBO --input ./generator_files/input_files/alliant_meta_data.csv --source_short_name aln

test:
	pytest -vvv -m "not integration_tests"

test-full:
	pytest -vvv

fmt:
	black .
	flake8 . --extend-ignore E203

type:
	mypy .

check: fmt type test

check-full: fmt type test-full