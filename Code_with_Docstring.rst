''' KommatiPara Application
    =======================

KommatiPara application has been created in order to conduct ETL process by using Pyspark Python API as follows:

- Extract: Reads two csv files from given paths and transform them into data frames.

- Transform: Filters one of the data frames according to selected countries, drops some columns which are unncessary for the company aim and renames columns for the easier readability to the business users. At the end, two data frames are joined according to the 'client_identifier' column. For the transformation process two generic functions which are 'filtering' and 'renaming' have been created.

- Load: Loads the output of the transformation to the given location.

For testing purposes, Chispa method has been used. 

Application receives three arguments from the users which are:

- The paths of the each datasets
- Countries that users want to filter
- Names that users want to give the columns

Additionally, the application keeps log records according to the rotating policy, the application rotates the log every day with a back up count of 5.
'''


.. code:: ipython3

    #Note: findspark.init(<Please change the path of spark folder in your computer)
    
    import findspark
    findspark.init("C:\spark-3.1.1-bin-hadoop2.7")

.. code:: ipython3

    #to import pyspark in order to create SparkSession
    
    import pyspark
    from pyspark.sql import SparkSession
    
    
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("KommatiPara_App") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

''' Pyspark libraries has been imported and used to transform dataframe because Apache Spark is a unified analytics engine for large-scale data processing and provides high-level APIs in Python 

'''

.. code:: ipython3

    import logging
    import time
    
    from logging.handlers import TimedRotatingFileHandler
    
''' The one of the purpose of this project is to keep log records and create rotation policy. In order to do this, a rotating policy has been created and according to the rotating policy, the application rotates the log every day with a back up count of 5
'''
    def logger():
'''Description
   ============
To keep log records according to the rotating policy.

return: 
	- stream handler
	- timedRotatingFileHandle
	- log handler
'''

        logger = logging.getLogger('kommatipara_log')
    
        logHandler = TimedRotatingFileHandler(filename="kommatipara_log", when="D", interval=1, backupCount=5)
        logFormatter = logging.Formatter('%(asctime)s %(name)-12s %(message)s')
        logHandler.setFormatter(logFormatter)
        logger.setLevel(logging.INFO)
    
    
        if not logger.handlers:
            streamhandler = logging.StreamHandler()
            streamhandler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
            streamhandler.setFormatter(formatter)
    
            logger.addHandler(streamhandler)
            logger.addHandler(logHandler)
    
        return logger 
    
    logger = logger()

.. code:: ipython3

    #Function for receiving paths of datasets
    def paths(*args):
''' Description
    ===========

	Receiving the paths of datasets

Parameters
	1. str(path1): the path of first dataset as a string
	2. str(path2): the path of second dataset as a string

Return
	dict(file_locations): the paths of the datasets as a dictionary format
'''
        path1 = input("Please enter the path of first dataset that you want to work on: ")
        path2 = input("Please enter the path of second dataset that you want to work on: ")
        logger.info("INFO: The paths of the datasets have been received.")
        file_locations = dict(location1 = path1, location2 = path2)
        return file_locations
    
    file_locations = paths()
    
    file_location1 = file_locations['location1']
    file_location2 = file_locations['location2']

.. code:: ipython3

    # to read first dataset
    import csv
    file_type = "csv"
    
    ''' To read first csv format dataset as a dataframe

    '''
    # CSV options
    infer_schema = "false"
    first_row_is_header = "true"
    delimiter = ","
    
    # The applied options are for CSV files. For other file types, these will be ignored.
    dataset_one = spark.read.format(file_type) \
      .option("inferSchema", infer_schema) \
      .option("header", first_row_is_header) \
      .option("sep", delimiter) \
      .load(file_location1)
    
    display(dataset_one)
    dataset_one.show()
    logger.info("INFO: {} has been read.".format(file_location1))

.. code:: ipython3

    #to read second dataset
    
    file_type = "csv"
    
    ''' To read second csv format dataset as a dataframe

    '''

    # CSV options
    infer_schema = "false"
    first_row_is_header = "true"
    delimiter = ","
    
    # The applied options are for CSV files. For other file types, these will be ignored.
    dataset_two = spark.read.format(file_type) \
      .option("inferSchema", infer_schema) \
      .option("header", first_row_is_header) \
      .option("sep", delimiter) \
      .load(file_location2)
    
    display(dataset_two)
    dataset_two.show()
    logger.info("INFO: {} has been read.".format(file_location2))

.. code:: ipython3

    #Function for filtering according to the selected countries
    #In this assignment, clients data only from the United Kingdom or the Netherlands is used.
    
    def filtering(var_1, var_2, logger):
''' Description
To filter first data frame according to the selected countries.

Parameters:
	1. First dataset as a data frame format
	2. The name of the countries as a dictionary format
	3. logger function for importing purposes

Return:
	1.Filtered dataset as a data frame format

''' 
	logger = logger
        dataset_one = var_1
        dic_of_country_name = var_2
        while True:
            try:
                while True:
                    number_of_country =''
                    total_country = len(dic_of_country_name)
                    number_of_country = int(input("Please enter number of country between 1 and {} that you want to filter:  ".format(total_country)))
                    if number_of_country > total_country or number_of_country < 1:
                        logger.warning('WARN: A number out of range has been entered [{}]'.format(number_of_country))
                        continue
                    break
                break
            except (ValueError):
                logger.exception('An incorrect entry has been entered [{}]'.format(number_of_country))
                continue
                
        if number_of_country == total_country:
            logger.info('INFO: All countries has been selected')
        else:
            country_list = []
            for i in range(0,number_of_country):
                while True:
                    try:
                        country_name = int(input('Please choose number of country [{}]: '.format(dic_of_country_name)))
                        if country_name not in dic_of_country_name:
                            logger.warning('WARN:  An incorrect entry has been entered [{}]'.format(country_name))
                            continue
                        else:
                            country_name = dic_of_country_name[country_name]
                            country_list.append(country_name)
                            logger.info('INFO: Country name [{}] has been added'.format(country_name))
                            break
                    except (ValueError):
                        logger.warning('WARN: An incorrect entry has been entered')
                        continue
        if country_list == None:
            pass
        else:
            filter_formula = ''
            for i in range(len(country_list)):
                filter_formula = filter_formula + "(dataset_one.country == country_list[" + str(i)+ "]) | "
            filter_formula = eval(filter_formula[:-2]) 
        dataset_one = dataset_one.filter(filter_formula)
        return dataset_one
    
    #to create a dictionary from 'country' column in order to choose country name for filtering
    value = [row[0] for row in dataset_one.select('country').distinct().collect()]
    key =[ x for x in range(1,(len(value)+1))]
    dic_of_country_name = dict(zip(key,value) )
    
    print(dic_of_country_name)
    
    dataset_one = filtering(dataset_one, dic_of_country_name, logger)
    dataset_one.show()
    logger.info("INFO: Dataset_one has been filtered according to the selected countries.")

.. code:: ipython3

    #to remove personal identifiable information from the first dataset, excluding emails
    
    dataset_one=dataset_one.drop("first_name","last_name","country")
    logger.info("INFO: First name, last name and country columns have been dropped.")
    dataset_one.show()

.. code:: ipython3

    # to remove credit card number column from the second dataset.
    
    dataset_two=dataset_two.drop("cc_n")
    logger.info("INFO: cc_n column has been dropped.")
    dataset_two.show()

.. code:: ipython3

    #Function for the renamimg
    #to rename the columns for the easier readability to the business users
    
    #id = client_identifier
    #btc_a = bitcoin_address
    #cc_t = credit_card_type
    
    def renaming(dataset_1,dataset_2, logger):
'''Description
   ===========
To rename the selected columns in order to make the readability easier

Parameters:
	1. First dataset as a dataframe
	2. Second dataset as a dataframe
	3. logger function for importing purposes

Return:
	1.dictionary format in order to keep the two renamed dataframes.
'''
        dataset_one = dataset_1
        dataset_two = dataset_2
        
        #to create a list from the column names
        column_list = dataset_one.columns
        column_list2 = dataset_two.columns
        for i in range (len(column_list2)):
            if column_list2[i] not in column_list:
                column_list.append(column_list2[i])
            else:
                pass
            
        #to create a list in order to keep new names for the column    
        value_list = []
        for i in range (len(column_list)):
            rename_column = input('Please enter the new name for {} column: (If do not want to change, please press ENTER)'.format(column_list[i]))
            if rename_column == '':
                value_list.append(column_list[i])
            else:
                value_list.append(rename_column)
        logger.info("INFO: New names for columns have been received.")
        
        #to rename colums for dataset_one and dataset_two
        new_names = dict(zip(column_list,value_list))
        for key, value in new_names.items():
            dataset_one = dataset_one.withColumnRenamed(key,value)
            dataset_two = dataset_two.withColumnRenamed(key,value)
        logger.info("INFO: Columns names have been changed.")
        
        dataset_one.show()
        dataset_two.show()
        datasets = dict(dataset_1 = dataset_one, dataset_2 = dataset_two)
        return datasets
    
    datasets = renaming(dataset_one, dataset_two, logger)
    dataset_one = datasets['dataset_1']
    dataset_two = datasets['dataset_2']

.. code:: ipython3

    # two datasets are joined according to the client_identifier 
    
    EnhancedDataset=dataset_one.join(dataset_two,dataset_one.client_identifier == dataset_two.client_identifier).select(dataset_one.client_identifier, dataset_one.email, dataset_two.bitcoin_address, dataset_two.credit_card_type)
    EnhancedDataset.show()
    logger.info("INFO: Two datasets has been joined.")

.. code:: ipython3

    #the output of this assignment is saved in a client_data folder.
    save_location = input('Please enter the save location ')
    
    EnhancedDataset.coalesce(1).write.mode("overwrite")\
    .format("com.databricks.spark.csv")\
    .option("header", "true")\
    .option("sep", ",")\
    .save(save_location)
    logger.info("INFO: Output has been saved in client_data folder.")

