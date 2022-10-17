# PPL Local Development Environment
This repo contains everything needed to set up a local development environments for data engineers.

The aim was to have one container to rule them all, however this was not practical as it made sense to use the MWAA runner which is maintained by AWS.

## PPL Local Development Container
PPL's standard local development IDE is [Visual Studio Code](https://code.visualstudio.com/) (VS Code) - a powerful, lightweight and highly configurable IDE maintained by Microsoft.  Using VS Codes remote containers functionality, a docker container is spun up, this container is configured with the standard PPL development build.  VS Code then interacts with the docker container.


# What Tools are included
- Python 3.9
- Python Snowflake Connector
- zip
- nano (text editor)
- Open JDK 8
- Glue + Spark
- dbt (v1)
- AWS CLI
- Pandas
- Argparse
- Mypy
- Pytest
- Black
- Flake8
- Virtual Environments

# High Level Steps
These are the high level steps that are required:
1. Install Docker Desktop
2. Install Visual Studio Code
3. Enable the [Remote - Container Extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
4. Create standard git repo location
5. Clone the development environment repo
6. Update .env file
7. Install and configure AWS CLI + Enable AWS Toolkit extension (optional)
8. Open the cloned folder from 5 in VS Code and start writing some cool code!

*All line endings are standardised as a unix not windows, you may need to change your git config if when cloning you see all files being modified.
you can do this by running --> git config --global core.autocrlf true*
*Note: .vscode/settings.json file contains a line ("files.eol": "\n") that will ensure that the correct line endings are used when you are coding in vscode*

## 1. Install Docker Desktop
PPL employees can contact the service desk to get Docker Desktop installed.  Use this [link](https://docs.docker.com/get-docker/) to install Docker Desktop for non PPL employees.  Simply select your OS and follow the instructions.

Once docker desktop is installed, you should [configure](https://docs.docker.com/desktop/windows/) it to use 4 or more GB RAM and 2 CPU cores. (note this may vary and can be adjusted based on your workload and user experience.)  By default when Docker Desktop is using WSL 2, Windows will handle the resources and thus no additional configuration is needed.


## 2. Install VS Code
PPL employees can contact the service desk to get VS Code installed, to make the most of the experience VS Code can give, this should be the latest version, at a minimum it needs to be version 1.4+ to make use of the remote containers extension.

For non PPL employees, use this [Link](https://code.visualstudio.com/docs/setup/setup-overview) and follow the instructions to install.


## 3. Enable the Remote - Container Extension
Use the following [Link](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) to install remote containers.

The following [Link](https://code.visualstudio.com/docs/remote/containers-tutorial) contains a tutorial and well worth a look :)


## 4. Create standard git repo location
We will all be working with BitBucket repo's and to make those repo's available inside and outside of the development environment we need to mount your local repo location to the development container.  

VS Code does this mounting for you automatically as part of the remote container experience, however we have had to standardise on a location on your local machine on where all the git repo's will live.  Please ensure you follow the below steps to set that location up, once done you can clone any repo's inside that location and work with them both inside and outside the development environment.

Steps:
1. Navigate to your ~ (home directory on mac/linux) or on windows C:\USERS\<user_name>
2. Create a folder called git
3. Clone any repo's in the git folder.

If you are interested, the mount location is in .devcontainer/devcontainer.json file which will be available after you clone the development environment repo in the next step.


## 5. Clone the development environment repo
Navigate to the folder you created in the above step.
Clone the XYZ repo.


## 6. Create .env file
Inside of the repo you created in step 5 there is a folder named ".devcontainer" and inside that folder is a file named ".env.example".

This contains the template for your .env file. On your sidebar, create a new file called ".env" in the ".devcontainer" directory.

The .env file is referenced during the container build process, it basically sets up environment variables that are used by various tools (e.g. dbt)
 
Copy what's in the .env.example file into your .env file, all you need to do is change your SNOWFLAKE_LOGIN and SNOWFLAKE_PASSWORD.

You can find your snowflake login by entering on a snowflake worksheet:
```
 select current_user();
```

You now need to rebuild the container for these changes to take effect.

Remember, any changes you make to the .env file will not take effect until you rebuild the container, so make sure if you change anything in future you rebuild the container afterwards!

### How to rebuild the container

1. Click on the status bar at the bottom of VSCode where you see:
'>< Dev Container: PPL Development Container'

2. Select 'Rebuild Container' from the input field's selectable list

## 7. Install and configure AWS CLI + Enable AWS Toolkit extension (optional)
VS Code has a cool extension for working with AWS Services.  The following steps will install the AWS CLI, mount the location and install the VS Code AWS Toolkit.

Steps:
1. Install the AWS CLI - > https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
2. Configure the AWS CLI -> https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html
3. Create a named profile called "ppluk-adp-d" -> https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html
    - Ensure you include the correct role_arn -> https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-role.html
    - Ensure that you have enabled MFA and enter the mfa_serial -> https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-role.html#cli-configure-role-mfa
4. To enable the AWS CLI toolkit extension you can simply add "AmazonWebServices.aws-toolkit-vscode" to the extensions list in your .devcontainer/devcontainer.json file.

*Your .aws/config file should look similar to this*
```
[default]
region = eu-west-1
output = json

[profile ppluk-adp-d]
role_arn = arn:aws:iam::920963906893:role/adp-supervisor
mfa_serial = arn:aws:iam::1234567891011:mfa/joe.blogs
source_profile = default
```


## 8. Open the cloned folder from 5 in VS Code and start writing some cool code!
Open VS Code and select File > Open Folder, navigate to you repo folder that you cloned in 5, once prompted, select re-open folder in container, once opened you can start coding :)

# Local environment variables testing and trouble shooting

The file test_env.py contains tests for the default .env config and tests to verify that certain packages are installed.

You can run all the config tests, see 'Run all config tests' or you can choose which tests to run.

### Run all config tests with pytest

This will check that your snowflake host, aws profile, dbt profile, snowflake role, database, schema and warehouse variables are all the default values, and will check that all your snowflake variables, including login and password, are not enclosed in double quotes, not empty strings and not None.

To navigate into the .devcontainer directory and run pytest, open a terminal and enter:
```
 cd .devcontainer
 pytest
```

To run pytest without navigating into the .devcontainer directory, open a terminal and enter:
```
 pytest .devcontainer/
```

Note:
pytest currently picks up and runs tests against any file in the current directory that contains
'test' in the file name

## To test that your snowflake host, aws profile and dbt profile are the same as the default

Open a terminal and enter:
```
 pytest --no-header -vvv .devcontainer/tests/test_env.py::test__environment_config__default_values_equal_current_values
```

This will test the snowflake host, aws profile and dbt profile configured in test_env.py against the default values and identify any changes from the default values.


## To test that your snowflake role, database, schema and warehouse are the same as the default

Open a terminal and enter:
```
 pytest --no-header -vvv .devcontainer/tests/test_env.py::test__snowflake_config__default_values_equal_current_values
```

This will test the snowflake role, database, schema and warehouse configured in test_env.py against the default snowflake values, the test will identify any changes from the default values.


## To test that your snowflake variables are not enclosed in double quotes, not empty strings and not None:

Open a terminal and enter:
```
 pytest --no-header -vvv .devcontainer/tests/test_env.py -k "variables"
```

This will test the snowflake login, password, role, database, schema and warehouse variables configured in test_env.py to ensure they are not enclosed in double quotes, not empty strings and not None.

You can run each test individually:

```
 pytest --no-header -vvv .devcontainer/tests/test_env.py::test__snowflake_variables__double_quotes
 pytest --no-header -vvv .devcontainer/tests/test_env.py::test__snowflake_variables__not_empty
 pytest --no-header -vvv .devcontainer/tests/test_env.py::test__snowflake_variables__not_none
```

### To test that the required packages are installed:

Open a terminal and enter:
```
 pytest --no-header -vvv .devcontainer/tests/test_env.py::test__container_packages__installed
```

This will retrieve the version of each package in the container_packages list in the test_env.py file and identify any packages that haven't been installed.


### pytest quick hints:

```
 pytest                                            ~~ runs all files containing "test"
 pytest -k "env"                                   ~~ runs any files containing "env"
 pytest path/test_env.py                           ~~ runs a specific test file
 pytest path/test_env.py::test_function_name       ~~ runs a specific test case
 pytest path/test_env.py -k "default"              ~~ runs any test cases containing "default"
 pytest path/test_env.py -k "variables"            ~~ runs any test cases containing "variables"
 pytest path/test_env.py -k "packages"             ~~ runs any test cases containing "packages"
```

### Issue: pytest output flags
The output of pytest tells you to use -v and -vv for more verbose explanations however, these flags don't always seem to work, sometimes using -vv prompts you to:
'Use -vv to get more diff'

So if that happens, then use -vvv:
```
 pytest -vvv .devcontainer/tests/test_env.py::test__environment_config__default_values_equal_current_values
```

### Issue: changes in .env file not being reflected in pytest output
If you make any changes in the .env file, you need to rebuild the container.

### How to rebuild the container

1. Click on the status bar at the bottom of VSCode where you see:
'>< Dev Container: PPL Development Container'

2. Select 'Rebuild Container' from the input field's selectable list


## Snowflake connection testing and trouble shooting

See 'Run all config tests with pytest' section above.

Make sure any changes to your .env file are saved and you have rebuilt the container.

Open a terminal and enter:
```
 python .devcontainer/tests/snowflake_connection.py
```
This will retrieve the current version of Snowflake.

## DBT connection testing and trouble shooting

Find the README in the adp-dbt-filestore repo to test and trouble shoot DBT:

/workspaces/adp-development-workspace/git-repos/adp-development-workspace/README.md

# Linting:
Linting is a useful tool for checking code for programmatic and stylistic errors. It ensures consistent code styling across the team that allows for more effective collaborating and easier debugging. The local dev container includes two Python linting packages, Black and Flake8, and one SQL linting package, SQLFluff.

## SQLFluff set up
Steps:
1. Go to the extension tab in the activity bar on the left hand side.
2. Find the 'sqlfluff' extension by dorzey under 'DEV CONTAINER: PPL DEVELOPMENT CONTAINER'
3. Click on the cog and open 'Extension Settings'
4. Under 'SQL > Linter: Executable Path' paste:
```
/usr/local/bin/sqlfluff
```
5. Under 'SQL > Linter: Run' choose when you want the linter to run, either on save or type.
6. You can also ad hoc lint your files with a keyboard shortcut by setting up a shortcut in 'Keyboard Shortcuts'

## Black
- PEP 8 compliant formatter
- How to use:
    - To run Black against one file, from the command line, run "black 'file-name'", e.g. 'black example.py'
    - To run Black against all files, run "black ."
    - The file will now have been re-formatted in line with the Black package's standards
    - For more information on its use, visit the Python Standards Confluence page https://ppluk.atlassian.net/wiki/spaces/ADP/pages/1047887954/Python+Standards#Black-and-Flake8

## Flake8
- PEP 8 complaint formatter
- How to use:
    - To run Flake8 against one file, from the command line, run "flake8 path/to/file.py", e.g.'flake8 C:\Users\User\Git\Repo\example.py
    - To run Flake8 against all files, run "flake8 ."
    - Flake8 will output the stylistic errors to the console, for you to then resolve
    - For more information on its use, visit the Python Standards Confluence page https://ppluk.atlassian.net/wiki/spaces/ADP/pages/1047887954/Python+Standards#Black-and-Flake8


# Useful Packages
- For more information on Python's use, visit the Python Standards Confluence page https://ppluk.atlassian.net/wiki/spaces/ADP/pages/1047887954/Python+Standards

## Pandas
- Pandas is a data analysis toolkit, which is installed as part of this local dev env container
- Imported to Python via 'import pandas as pd'
- See the documentation for useful pandas methods https://pandas.pydata.org/docs/user_guide/index.html#user-guide

## Argparse
- Argparse makes it easier to write user-friendly command-line interfaces.
- Define required arguments, and argparse will parse those out to the console is a more readable interface
- For more information on its use, visit https://docs.python.org/3/library/argparse.html

## Mypy
- Mypy is a static type checker for Python. Because Python is dynamic, erros are usually only detected when the code is ran. Mypy finds bugs before the code is ran
- How to use:
    - From the command line, run "mypy file-name", e.g. 'mypy example.py'
    - To run mypy against all files, run "mypy ."
    - Mypy will then provide hints in the code to make it run as expected
    - For more information on its use, visit https://mypy.readthedocs.io/en/latest/getting_started.html#installing-and-running-mypy

## Pytest
- Allows writing small, readale tests to support functional testing
- Test functions output the exected result by running "pytest file-name", e.g., 'pytest example.py' on the command line
- For more information on its use and defining tests, visit https://docs.pytest.org/en/7.1.x/getting-started.html#getstarted

## Virtualenv
- Virtual environments are a useful tool for managing libraries between projects. Specific projects may have different library requirements, and so errors will arise when trying to develop code in multiple environments with different requirements. Virtual environments solve this by allowing you to install libraries to a specific Python environment, instead of your local machine. This means you can have multiple versions of a library installed on your machine, each in its own virtual environment.
- For more information on its use and how to create a virtual env, visit https://docs.python.org/3/tutorial/venv.html
