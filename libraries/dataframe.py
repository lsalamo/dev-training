from typing import overload
import pandas as pd
import numpy as np
from functools import reduce


class Dataframe:
    def __init__(self):
        pass

    @staticmethod
    def is_empty(df):
        return df.empty

    @staticmethod
    def join_by_index_row(frames, join):
        df = pd.concat(frames, axis=1, join=join)
        return df

    @staticmethod
    def join_by_index_column(frames, join):
        df = pd.concat(frames, axis=0, join=join)
        return df

    class Cast:
        def __init__(self):
            pass

        @staticmethod
        def columns_regex_to_int64(df, regex):
            """ CALL >
            df_values = Dataframe.Cast.columns_regex_to_int64(df, '(-aa|-ga)$')
            df[df_values.columns] = df_values
            """
            df = Dataframe.Columns.select_columns_by_regex(df, regex)
            return df.astype(np.int64)

    class Rows:
        def __init__(self):
            pass

    class Columns:
        def __init__(self):
            pass

        @staticmethod
        def exists(df, column):
            """ CALL > Dataframe.Columns.exists(df, 'day') """
            if column in df.columns:
                return True
            else:
                return False

        @staticmethod
        def columns_names(df):
            """ CALL > Dataframe.Columns.columns_names(df) """
            return list(df.columns)

        @staticmethod
        def select_columns_by_regex(df, regex) -> pd.DataFrame:
            """ CALL > Dataframe.Columns.select_columns_by_regex(df, '(-aa|-ga)$') """
            return df.filter(regex=regex, axis=1)

        @staticmethod
        def join_two_frames_by_index(frame_left, frame_right, join) -> pd.DataFrame:
            """ CALL > Dataframe.Columns.join_two_frames_by_index(fr1, fr2, 'outer') """
            return pd.merge(left=frame_left, right=frame_right, left_index=True, right_index=True, how=join)

        @staticmethod
        def join_two_frames_by_columns(frame_left, frame_right, columns, join, suffix) -> pd.DataFrame:
            """ CALL > Dataframe.Columns.join_by_columns(fr1, fr2, ['a','b'], 'outer', ('-fr1', '-fr2')) """
            return pd.merge(left=frame_left, right=frame_right, on=columns, how=join, suffixes=suffix)

        @staticmethod
        def join_by_columns(frames, columns, join) -> pd.DataFrame:
            """ CALL > Dataframe.Columns.join_by_columns([fr1, fr2, ...], ['a','b'], 'outer') """
            df = reduce(lambda left, right: pd.merge(left, right, on=columns, how=join), frames)
            df = df.fillna(0)
            return df

        @staticmethod
        def drop(df, columns, inplace):
            """ CALL > Dataframe.Columns.drop_columns(df, ['a','b'], True) """
            # df.loc[:, df.columns != 'b']
            # df[df.columns.difference(['b'])]
            return df.drop(columns, axis=1, inplace=inplace)

        @staticmethod
        def drop_from_index(df, index, inplace):
            """ CALL > Dataframe.Columns.drop_columns_from_index(df, 4, True) """
            return df.drop(df.loc[:, index:], axis=1, inplace=inplace)

        @staticmethod
        def drop_to_index(df, index, inplace):
            """ CALL > Dataframe.Columns.drop_columns_from_index(df, 4, True) """
            return df.drop(df.loc[:, :index], axis=1, inplace=inplace)

        @staticmethod
        def split_column_string_into_columns(df, column, char):
            """ CALL > Dataframe.Columns.split_column_string_into_columns(df, 'column', ',') """
            df_values = df[column].str.split(char, expand=True)
            return Dataframe.Columns.join_two_frames_by_index(df, df_values, 'inner')

        @staticmethod
        def split_column_array_into_columns(df, column):
            """ CALL > Dataframe.Columns.split_column_array_into_columns(df, 'column', ',') """
            df_values = pd.DataFrame(df[column].tolist(), index=df.index)
            df = Dataframe.Columns.join_two_frames_by_index(df, df_values, 'inner')
            return df


