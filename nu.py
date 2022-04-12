import pandas as pd
import functools

def col_replace(col):
    try:
        replace_string = ('.', '_'), (' ', '_'), (',', ''), ('(', ''), (')', ''), ('{', ''), ('}', ''), (';', ''), (
            ':', ''), ('/n', '_'), ('/t', '_'), ('/', '_'), ('[', ''), (']', '')
        return functools.reduce(lambda a, kv: a.replace(*kv), replace_string, col)
    except Exception as error:
        raise Exception("Error in col_replace | %s" % error)

def parquet_convert(d_frame):
    d_frame.to_parquet('output.parquet')

if __name__ == '__main__':
    try:
        df = pd.read_csv("/Users/dishamukherjee/Downloads/Pragma-datasets/Footfall.csv")
        # print(df.convert_dtypes().dtypes)
        # print(df.dtypes)
        for col_name in df.columns:
            df.rename(columns={col_name: col_replace(col_name)}, inplace=True)
            # if col_name in "date" or col_name in "Date":
            #     pass
            # else:          
            if col_name in "date" or col_name in "Date":
                pd.to_datetime(df[col_name])
        # df= df.convert_dtypes().dtypes   
        print(df.dtypes)
            # if df.dtypes in "int" : #   or df.dtypes in "float":
            #     df[col_name].fillna(0, inplace=True)
            # else:
            #     df[col_name].fillna("No_Data", inplace=True)
        parquet_convert(df)
    except Exception as Error:
        raise Exception("Error in Main function | %s" % Error)


