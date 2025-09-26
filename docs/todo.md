
support:
- jupyter
- docs
- tests with pytest
- 


# commands for testing

test:

here to change my_py_template with the name of the repo
    pytest -vvv --cov-report=term-missing --cov-config=pyproject.toml --cov=my_py_template --cov=tests --ignore=./tests/test_examples.py --ignore=./tests/test_notebooks.py



test-examples:
    pytest -vvv ./tests/test_examples.py

test-docs:
    mkdocs build --clean --strict

# explain how to modify the file .github/workflows/ci.yml in order to integrate documentation + support to the page github.io

# todo what is the file .coverage, try to understand if you can remove it 
# todo add a link to the reference letter at the cv, the link should be in my web site
# todo add more reference to the web site also in the readme


# say that if you remove the hello world file you have to chenge the test o delete the tests | how to delete the tests?

# todo: add pytest in gitlab pipeline




