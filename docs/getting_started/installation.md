# Activation of the repository

Here you have a detail guide to install and use the repository

## 🚀 Getting Started

### 1. Clone the repository
```bash
cd ../working_folder
git clone https://github.com/giovanniminuto/my-py-template.git
cd my-py-template

# activate you favorite IDE 
# Es: VS Code 
code .

```

#### Note: 
- This is a good time to rename the project from my-py-template to your desired project name.
You can find all occurrences of the template name in tracked files with:
```bash
git grep -niE "my[-_]py[-_]template" 
```
where: 
- - `git grep` - Print lines matching a pattern, documentation [link](https://git-scm.com/docs/git-grep)
- - `-n` prints matches along with the line number inside each file.
- - `-i` makes the search case-insensitive
- - `-E` enables extended regex -> regex code [-_]:  means “either a dash (-) or an underscore (_)”.

And all the file name contains the template name
```bash

git ls-files | grep -iE "my[-_]py[-_]template"
```
- - `git ls-files` - Show information about files in the index and the working tree, documentation [link](https://git-scm.com/docs/git-ls-files)
- - `-i` makes the search case-insensitive
- - `-E` enables extended regex -> regex code [-_]:  means “either a dash (-) or an underscore (_)”.


- If you want to copy the files into an existing repository, check out [this](https://stackoverflow.com/questions/71830565/how-can-i-copy-code-from-one-code-repository-to-another-in-foundry) practical guide on StackOverflow. 


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
```bash
pip install -e .
```
ℹ️ Requires pip >= 21.3
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







