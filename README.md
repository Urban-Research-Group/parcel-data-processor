[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# Parcel Data Processor (GA Tax Assessment Project)
![img](/docs/county_assessment_pipeline.png "Complete Data Pipeline")
## Tax assessment data process and analysis for counties in GA, specifically those around metro Atlanta. Affiliated with the Urban Research Group at Georgia Tech.

### Installing Requirements
Before running the program, make a virtual environment from the provided requirements.txt file.

Option 1, conda: 
```bash
conda env create -f requirements.txt
```
Option 2, venv:
```bash
pip install -r requirements.txt
```
If you can't get this to work, you can manually run the following command for each package included in requirements.txt.
```bash
pip install <requirement>
```

### How to Run
```bash
python src/processor <config_path> <execution_name>
```
<config_path>: path to YAML file with execution instructions \
<execution_name>: name given to identify the current execution
