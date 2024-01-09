# Vehicles Crash Analysis

## Details

1) `Data` directory contains all the required data for performing the analysis
2) `Data_Processor` directory includes a module where the class and all the methods for analysis are defined
3) `config.yaml` file serves as the configuration YAML file, where the paths for input and output files are defined. These configurations are read by the main driver and module from the same config file
4) `main.py` file contains the main method where all the required methods are called to execute the analysis
5) To run the module, you can clone this repository and use the spark-submit --master local[2] main.py command, which will generate the Output directory
6) `Output` is where all the results will be stored in the form of CSV files
