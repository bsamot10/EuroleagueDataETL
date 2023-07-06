### Description

A Python tool that creates one _csv_ file for each available table. 
Each _csv_ file includes all the content of the table. 

### Command line arguments
* --competition, -cp

The input is _euroleague_ (default) or _eurocup_.

### Results

The results are saved in the EuroleagueDataETL directory under a folder with name _data_ 
and they are classified in sub-directories per competition.

### Example 1 (default execution)

Create _csv_ files for the Euroleague competition:

```python main.py```

![default_execution_screenshot](https://github.com/bsamot10/EuroleagueDataETL/blob/main/docs/images/tables_to_csv_example_1.png)

### Example 2

Create _csv_ files for the Eurocup competition:

```python main.py -cp eurocup```

![default_execution_screenshot](https://github.com/bsamot10/EuroleagueDataETL/blob/main/docs/images/tables_to_csv_example_2.png)
