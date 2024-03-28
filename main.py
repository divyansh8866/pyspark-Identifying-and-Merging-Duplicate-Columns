from pyspark.sql import SparkSession
import pyspark.sql.functions as f

class SparkDfCleaner():
    def __init__(self, spark_df):
        self.df = spark_df
        self.metadata_dict = {}
        self.column_names = [item.lower() for item in self.df.columns]
        
    def identify_duplicate_col(self):
        duplicate_columns = set()
        seen = set()
        for col in self.column_names:
            col_name = col.lower()
            if col_name in seen:
                duplicate_columns.add(col_name)
            seen.add(col_name)

        if duplicate_columns:
            # Duplicate columns are found, store the information
            duplicate_col_dict = {}
            for idx, value in enumerate(self.column_names):
                if value.lower() in duplicate_columns:
                    try:
                        duplicate_col_dict[value.lower()].append(idx)
                    except:
                        duplicate_col_dict[value.lower()] = []
                        duplicate_col_dict[value.lower()].append(idx)
            self.metadata_dict = duplicate_col_dict
        else:
            # No duplicate columns found
            print(">> No duplicate column names found.")
            
    def merge_duplicate_col(self):
        try:
            for key, value in self.metadata_dict.items():
                for i in value:
                    self.column_names[i] = key + '_duplicate_' + str(i)
            self.df = self.df.toDF(*self.column_names)
            for key, value in self.metadata_dict.items():
                duplicate_columns = []
                for i in value:
                    col_name = key + "_duplicate_" + str(i)
                    duplicate_columns.append(col_name)
                duplicate_col = [f.col(i) for i in duplicate_columns]
                self.df = self.df.select('*', f.concat_ws(' ', *duplicate_col).alias(key))
                self.df = self.df.drop(*duplicate_columns)
        except Exception as e:
            print(e)
            
    def return_df(self):
        return self.df
    
if __name__ =="__main__":
    # Show the original dataframe
    df.show()
    # Create an instance of the SparkDfCleaner class
    handler = SparkDfCleaner(df)
    # Identify duplicate columns in the DataFrame
    handler.identify_duplicate_col()
    # Merge duplicate columns in the DataFrame
    handler.merge_duplicate_col()
    df = handler.return_df().show()
