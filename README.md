# Vehicles Crash Analysis

## Details

1) `Data` directory has all the required data to perform the analysis
2) `Data_Processor` directory has a module were the class is define and all the methods are define to perform the analysis
3) `config.yaml` is the config yaml file were the input file's path and output file's path is define which is config driver and module will take it from the same config file
4) `main.py` is main method were all the methods are call to run it
5) You can clone this repo and use `spark-submit --master local[2] main.py` command to run the module which will generate the Output directory
6) `Output` directory is were all the results will be stored in form of csv files
