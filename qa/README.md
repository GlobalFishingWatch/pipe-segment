# QA Notebook Overview

* `config_old.py`: Used to power scripts that have not been updated in the final round of QA for pipe 3.

* `config.py`: Used to power the main `segmenter_version_comparison.py` QA notebook.

* `segmenter_basic_checks.py`: This notebook will be ported into automated daily queries within the Airflow process and can be ignored if it still exists here. This script uses `config_old.py` and should be updated at some point.

* `segmenter_time_mode_comparison.py`: Checks that the output of the segmenter is exactly the same when run on the same set of data with the same parameters except the time mode (i.e. all backfill, yearly, monthly, daily). Tweaks may need to be made for table names and time modes as the focus was on yearly and monthly only. Check data costs before running as this notebook was developed only on one year of the baby pipe (small subset of MMSI). This script uses `config_old.py` and should be updated at some point.

* `segmenter_version_comparison.py`: Computes dozens of metrics to compare between two pipelines including daily metrics from the `segments_` and `segment_identity_daily_` tables and SSVID level metrics from the `segment_info` table. This is the main QA notebook and all figures and printouts should be examined for differences between the pipelines that may need to be further explored and/or documented.


# How to Use the QA Notebooks

1. Create a new folder under `runs/` called `YYYYMMDD/` with the date of the pipeline run you are QAing.
2. Copy the notebooks in `templates/` into that new folder. Note that the main QA notebook is `segmenter_version_comparison.py` so this may be the only one you want to copy along with `config.py`. If you have two versions run in different time modes for the same time period, you'll also want `segmenter_time_mode_comparison.py`. `segmenter_basic_checks.py` will eventually be integrated as automatic daily checks in the Dataflow process so this can largely be ignored for now.
3. Update the tables in `config.py` to match what you are comparing/QAing.
3. Modify your copy of the notebooks templates as needed.
4. After final run, export your notebook to Markdown for permanent storage of the outputs that will exist even when the tables you are using no longer do. Be sure to commit the following:
    * Any image folders that have been created to power the markdowns which will be in a file that matches the name of the notebook
    * The data folder to preserve the data that was used to generate the analysis and some outputs that are data only and not figures
You may need to use `git add -f` to forcefully add the files in these folders against the `.gitignore` but do so carefully and slowly.
_Note: No need to commit the figures folder as these will be in the folder that powers the markdown although they will be lower quality. The high resolution outputs in `figures/` are for your use._

### Exporting to Markdown

1. **Command line:** run `jupyter nbconvert --to markdown <notebook.ipynb>`
_Note: it can be tricky in VSCode to get a .ipynb to save out if you are using libraries to work directly with .py but as a notebook. One thing you can do is go to *File > Save As* and save as a slightly different name with a .ipynb ending. You can rename it back before converting to markdown. You have to save under a different name or it may not actually save the file as a new . ipynb depending on your VSCode set up._

2. **Web-based interface:** Navigate to *File > Download As* and select Markdown.

See [this link](https://reproducible-science-curriculum.github.io/publication-RR-Jupyter/02-exporting_the_notebook/index.html) for more information.

*Note: you may need to install the `jupyter-contrib-nbextensions` if you haven't done so yet (see [instructions](https://jupyter-contrib-nbextensions.readthedocs.io/en/latest/install.html))*.
