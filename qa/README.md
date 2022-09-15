# How to Use the QA Notebooks

1. Create a new folder under `runs/` called `YYYYMMDD/` with the date of the pipeline run you are QAing.
2. Copy the notebooks in `templates/` into that new folder.
3. Update the tables in each notebook to match what you are comparing/QAing.
3. Modify your copy of the notebooks templates as needed.
4. After final run, export your notebook to Markdown for permanent storage of the outputs that will exist even when the tables you are using no longer do. Make sure to commit any image folders that have been created as well.

### Exporting to Markdown

1. **Command line:** run `jupyter nbconvert --to markdown <notebook.ipynb>`

2. **Web-based interface:** Navigate to *File > Download As* and select Markdown.

See [this link](https://reproducible-science-curriculum.github.io/publication-RR-Jupyter/02-exporting_the_notebook/index.html) for more information.

*Note: you may need to install the `jupyter-contrib-nbextensions` if you haven't done so yet (see [instructions](https://jupyter-contrib-nbextensions.readthedocs.io/en/latest/install.html))*.
