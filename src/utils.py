import logging
from pyspark.sql.functions import col, count, when

# create logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create console handler and set level to INFO
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to console handler
ch.setFormatter(formatter)

# add console handler to logger
logger.addHandler(ch)

class DataPreprocessor:

    def __init__(self, df, threshold=15):
        self.df = df
        self.threshold = threshold
        self.logger = logging.getLogger(__name__)
    
    def count_rows(self,df):
        if df.count()>0:
            self.logger.info('Valid Input')
        else:
            self.logger.info('Corrupted input')

    def num_columns(self,df,col:int):

        if len(df.columns) == col:
            self.logger.info('Valid number of columns')
        elif len(df.columns) > col:
            self.logger.info('Exceeded number of valid columns')
        else:
            self.logger.info('Less than number of valid columns')


    def get_nulls(self,df,threshold):
        """
        A function to get the overall overview of nulls in the dataset
        with option to set the suitable threshold.
        """
        self.logger.info('Getting null report')

        fields_with_nulls = []
        nulls_below_thres = []

        #get number of nulls
        nulls = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        nulls_percentage = nulls.toPandas().transpose()
        nulls_percentage['percent'] =(nulls_percentage[0]/df.count())*100

        for index,row in nulls_percentage.iterrows():
            if row['percent']>0 and row['percent']<= threshold:
                nulls_below_thres.append((index,row['percent']))
            if row['percent']> threshold:
                fields_with_nulls.append((index,row['percent']))

        return fields_with_nulls, nulls_below_thres

    def null_status(self,threshold, above_thres:list,below_thres:list):
        #drop nulls
        if len(above_thres)>0:
            self.logger.warning('Significant amount of Nulls detected')
            self.logger.info(f'Columns with high number of nulls are {above_thres}')
        else:
            self.logger.info('No nulls above threshold detected')

        if len(below_thres)>0:
            self.logger.info(f'The number of nulls in columns below {threshold}% are {below_thres}')
        else:
            self.logger.info('No nulls below threshold')

        return

    def remove_nulls(self,df, fields_with_nulls:list, rows=False):
        if rows:
            sub = [i[0] for i in fields_with_nulls]
            no_nulls = df.na.drop(subset= sub)
            number_of_rows_removed = df.count() - no_nulls.count()
            percent_of_numbers_removed = (number_of_rows_removed/df.count())*100
            self.logger.info(f'The number of rows removed is {number_of_rows_removed} which is the {percent_of_numbers_removed}%')
        else:
            no_nulls = df.drop(*[column[0] for column in fields_with_nulls])
            self.logger.info(f'The columns removed {fields_with_nulls}')

        return no_nulls

    def check_duplicates(self,df):
        if df.count() > df.dropDuplicates([c for c in df.columns]).count():
            self.logger.warning('Data has duplicates')
            no_duplicates = df.dropDuplicates()
            number_of_duplicates = df.count()-no_duplicates.count()
            percent_of_duplicates = (number_of_duplicates/df.count())*100
            self.logger.info(f'Number of duplicates removed is {number_of_duplicates} which is the {percent_of_duplicates}% of dataset')
            return no_duplicates
        else:
            self.logger.info('Dataset has no duplicates')
            return df