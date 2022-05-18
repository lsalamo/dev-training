import pandas as pd
from functools import reduce


class Dataframe:
    @staticmethod
    def join_by_index_row(frames, join):
        df = pd.concat(frames, axis=1, join=join)
        return df

    @staticmethod
    def join_by_index_column(frames, join):
        df = pd.concat(frames, axis=0, join=join)
        return df

    class Columns:
        @staticmethod
        def select_columns_by_regex(df, regex):
            """ CALL > Dataframe.Columns.select_columns_by_regex(df, '(-aa|-ga)$') """
            df = df.filter(regex=regex)
            return df

        @staticmethod
        def columns_names(df):
            """ CALL > Dataframe.Columns.columns_names(df) """
            return list(df.columns)

        @staticmethod
        def join_by_columns(frames, columns, join):
            """ CALL > Dataframe.Columns.join_by_columns([fr1, fr2, ...], ['a','b'], 'outer') """
            df = reduce(lambda left, right: pd.merge(left, right, on=columns, how=join), frames)
            return df

        @staticmethod
        def join_two_frames_by_columns(frame_left, frame_right, columns, join, suffix):
            """ CALL > Dataframe.Columns.join_by_columns(fr1, fr2, ['a','b'], 'outer', ('-fr1', '-fr2') """
            df = pd.merge(left=frame_left, right=frame_right, on=columns, how=join, suffixes=suffix)
            return df

        @staticmethod
        def drop_columns(df, columns, inplace):
            """ CALL > Dataframe.Columns.drop_columns(df, ['a','b'], True) """
            # df.loc[:, df.columns != 'b']
            # df[df.columns.difference(['b'])]
            df.drop(columns, axis=1, inplace=inplace)
            return df
