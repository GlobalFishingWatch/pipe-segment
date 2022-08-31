# How to Use the QA Notebooks

1. Create a new folder under `runs/` called `YYYYMMDD/` with the date of the pipeline run you are QAing.
2. Copy the notebooks in `templates/` into that new folder.
3. Update the tables in each notebook to match what you are comparing/QAing.
3. Modify your copy of the notebooks templates as needed.
4. After final run, export your notebook to rendered HTML for permanent storage of the outputs that will exist even when the tables you are using no longer do.

### Exporting to HTML

1. [In VSCode](https://code.visualstudio.com/docs/datascience/jupyter-notebooks#:~:text=You%20can%20export%20a%20Jupyter,dropdown%20of%20file%20format%20options.)  
*Note: you may need to install the `jupyter-contrib-nbextensions` if you haven't done so yet (see [link](https://jupyter-contrib-nbextensions.readthedocs.io/en/latest/install.html))*

2. [In Jupyter Notebook web interface](https://mljar.com/blog/jupyter-notebook-html/)

3. [In Jupyter Lab web interface](https://jupyterlab.readthedocs.io/en/stable/user/export.html)

3. On the command line: run `jupyter nbconvert --to html yourNotebook.ipynb`
