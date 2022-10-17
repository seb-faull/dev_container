# Summary

This folder contains the ADP Generator. The aim of this application is to generate the DDL and DBT artefacts needed to
load the ADP.

# Quickstart

If run inside the Workspace container then no additional requirements need to be installed.

To run the generator, follow these steps:
1. Add a CSV file containing the input metadata for the objects to be generated to the 'input_files' folder
2. Run the generator using the follow terminal command:
> python ./adp_generator.py --action generate_all_objects --source [source] --schema [schema] --input [input_file]

Where
* [source] is the name of database where source tables are
* [schema] is the name of schemas where source tables are
* [input_file] is the path to the input file added in step 1

# Repo Layout

The entrypoint for the application is 'adp_generator.py'.

The 'configuration' folder contains files which drive the behaviour of the generator, but does not include the metadata input files. For example, this contains the 'invalid_tables_names.py' file, which captures a list of invalid names to check for in input files.

The 'input_files' folder contains a set of CSVs that hold the metadata for tables, which is used to drive the generator and provide the needed values to be substituted into templates.

The 'src' folder contains general application source code. This includes the 'adapters' folder, which contains adapters for interacting with external systems (see 'Hexagonal Architecture').

The 'templates' folder contains the parameterisable DDL and DBT templates that are used by the generator.

The 'tests' folder contains the PyTest test files. This also includes an 'assets' folder, which holds assets used in the tests, and a 'mocks' folder which contains mocked / patched versions of the codebase which assist with isolating other tests.

# Debugging

The configuration for debugging the generator can be found in the .launch.json file.

To start a debug session:

1. Open "Run and Debug" in the activity bar in the far left (the icon that looks like a 'play' button with a little beetle) or do Ctrl + Shift + D

Your screen will look like this: https://code.visualstudio.com/docs/editor/debugging

2. In the newly opened debug side bar, at the top where it says 'RUN AND DEBUG', click on the text between the little green 'play' icon and the cog icon to open up the drop down, then select the program file you want to debug, which in this case is “Python: adp_generator.py”

# Development / Testing

If run inside the Workspace container then no additional requirements need to be installed to develop the codebase.

A Makefile exists in this folder. It contains commands that assist with formatting, type checking, and running tests.

To run the linters / formatters, MyPy, and test cases, run the following command in the terminal:
> make check

Prior to committing any changes, this command should be run to ensure that the code is still valid, and adheres to code standards.