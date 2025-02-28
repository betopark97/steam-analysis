# Roberto's Cookiecutter Data Science

*My own standardized project structure for data science projects.*

> This is personalized to my own comfort and may not be the best practice for organizing a project structure.

## Installation & Setup
**Step 1**: Clone the repository to your local environment.
``` {bash}
git clone https://github.com/betopark97/cookiecutter-data-science.git
```
**Step 2**: Check for existing remotes, if not initialize git or connect to your own repository in the next step.
``` {bash}
git remote -v
```
**Step 3**: Replace the existing origin (remote) with the following command (`<new_url>` is a placeholder for the repository where you want to push with this cookiecutter).
``` {bash}
git remote set-url origin <new_url>
```
**Step 3-1 (optional)**: This step is instead of the previous step. It will remove an existing origin and add a new one.
``` {bash}
git remote remove origin
git remote add origin <new_url>
```
**Step 4 (optional)**: This step is to erase the .gitkeep files in the empty directories. You may choose to keep them if you want to and skip this step.
``` {bash}
python remove_gitkeep.py
```

**Step 5**: This step is the erase the .git file in this directory. If you fork a copy or git clone this repository for your projects, the git log will show the history of this repository. That is why if you want to start clean you must run this python file.
``` {bash}
python reset_git.py
```

## Project Structure
After cloning this repository, the structure of the data science project should look like the following:  

> The tree below comes with an explanation of the use case of each of the subdirectories and files.

``` {bash}
├── LICENSE                 -> MIT License (mine)
├── README.md               -> Explanation of Project
├── data                    -> Data for Project
│   ├── external            -> Data from third party sources
│   ├── interim             -> Intermediate data that has been transformed
│   ├── processed           -> Final dataset for modeling
│   └── raw                 -> Original, immutable dataset
├── databases               -> Connection to databases, or store databases
├── misc                    -> Any miscellaneous files
├── models                  -> Trained, serialized models
├── notebooks               -> Jupyter notebooks
├── references              -> Data catalogue, manuals or materials
├── remove_gitkeep.py       -> Python file to remove .gitkeep files from directory
├── reports                 -> Final deliverable of the project
│   └── figures             -> Visual aids (plots, graphs) for the final deliverable
├── requirements.txt        -> Requirements to reproduce the packages necessary for the project
├── src                     -> Source code for the project
│   └── utils               -> Utilities for project source code
└── tests                   -> Tests for different features of the project
```

## Final Words
This project will be updated frequently. The structure in this cookiecutter 
is mostly for static projects, so keep that in mind and use it accordingly.

Future updates:
1. Apply CRISP-DM structure to notebooks.
2. Apply a framework for the src (source) code.
3. Apply specific tools to be used along with the cookiecutter.
4. Apply a Workflow example with the tools used for a data science project.
5. Update the usage instructions for better productivity.