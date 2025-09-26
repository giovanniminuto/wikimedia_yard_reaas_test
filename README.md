# MY-PY-TEMPLATE

[![CI](https://github.com/giovanniminuto/my-py-template/actions/workflows/ci.yml/badge.svg)](https://github.com/giovanniminuto/my-py-template/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/giovanniminuto/my-py-template?logo=codecov&style=flat-square)](https://codecov.io/gh/giovanniminuto/my-py-template)
[![Docs](https://img.shields.io/badge/docs-mkdocs--material-blue?style=flat-square&logo=markdown)](https://giovanniminuto.github.io/my-py-template/)
[![License](https://img.shields.io/github/license/giovanniminuto/my-py-template?style=flat-square)](./LICENSE)

A **minimal Python project template** with a pre-configured `pyproject.toml` to get started quickly.  
It includes ready-to-use configurations for:

- **Type checking** → [mypy](https://mypy.readthedocs.io/)  
- **Testing** → [pytest](https://docs.pytest.org/)  
- **Code quality** → [pre-commit](https://pre-commit.com/)  
- **Docs** → [mkdocs](https://www.mkdocs.org/) with [mkdocs-material](https://squidfunk.github.io/mkdocs-material/) and [mkdocstrings](https://mkdocstrings.github.io/)  
- **CI/CD** → GitHub Actions workflows  

This repository is intended as a **clean slate** for new projects: lightweight, flexible, and easy to extend.


## 🚀 Getting Started

### 1. Clone the repository
```bash
cd ../working_folder
git clone https://github.com/giovanniminuto/my-py-template.git
cd my-py-template
```

### Optional: 2. Disconnect the template repository `giovanniminuto/my-py-template.git` and connect your own

First, check your current remotes:
```bash
git remote -v
```

You should see something like:
```bash
origin  git@github.com:giovanniminuto/my-py-template.git (fetch)
origin  git@github.com:giovanniminuto/my-py-template.git (push)
```

*Remove the template remote*
To disconnect from the original template, run:

```bash
git remote remove origin
```

Verify that the remote was removed:

```bash
git remote -v
```
This should return nothing.

*Add your own repository*
Now connect your repository:

```bash
git remote add origin git@github.com:username/my-repo.git
```

Push the code to your repository:

```bash
git push -u origin main 
```
At this point, all files from the template should appear in your repository.

#### Note: 
- This is a good time to rename the project from my-py-template to your desired project name.
You can find all occurrences of the template name in tracked files with:
```bash
git grep -niE "my[-_]py[-_]template" 
```

And all the file name contains the template name
```bash
git ls-files | grep -iE "my[-_]py[-_]template"
```

*Now it's time to push changes:*

Once you’ve made your edits, commit and push them:

```bash
git add . 

git commit -m "Initial project setup" 

git push
```


### 2. Create a virtual environment

```bash
python -m venv .venv
```
The .venv/ folder is ignored by Git by default.
If you change the environment folder name, update .gitignore accordingly.
Activate the environment:
- Linux/macOS
```bash
source .venv/bin/activate
```
- Windows (PowerShell)
```bash
.venv\Scripts\Activate.ps1
```
### 3. Install dependencies
Install project dependencies:
ℹ️ Requires pip >= 21.3
```bash
pip install -e .
```
The default dependencies include Qadence ([link](https://github.com/pasqal-io/qadence)) and Jupyter Notebook support.
You can edit them under [project.dependencies] in pyproject.toml.

Install the optional dependencies to use pre-commit/pytests/mkdocs:
```bash
pip install -e ".[dev,docs]"
```

### 4. Set up pre-commit hooks
Enable automatic checks on commit:
```bash
pre-commit install
```


## 📂 Project Structure

Now everything is set up, and you can start adding your source code inside:

- **`my_py_template/`** → your main source code  
- **`examples/`** → example scripts that use your source code  

## Here the last info to run properly the Pre-commit, Tests and Docs

### 🧹 Pre-commit Hooks

Run all pre-commit hooks manually:
```bash
pre-commit run --all-files
```

### 🧪 Running Tests
Run all tests with:
```bash
pytest 
```

### 📖 Documentation
Build documentation locally with:
```bash
mkdocs serve
```
Then open http://127.0.0.1:8000.

Validate documentation build (strict mode):
```bash

mkdocs build --clean --strict
```

### ⚙️ CI/CD

This template includes a ready-to-use **GitHub Actions workflow** for:

- Linting & testing  
- Building documentation  
- Optional deployment to GitHub Pages  

See [`.github/workflows/ci.yml`](./.github/workflows/ci.yml).


### 📝 License

This project is licensed under the [MIT License](./LICENSE).

### 💡 Contributing
Feel free to open issues and pull requests to improve this template!


