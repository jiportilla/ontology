#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from base import MandatoryParamError


class FetchBlogInterestUsers(BaseObject):
    """ Fetch Interested Users of Blog post """

    def __init__(self,
                 df_src: DataFrame,
                 selected_blog_url: str):
        """
        Created:
            - 13-July-2019
            - abhbasu3@in.ibm.com
        """
        BaseObject.__init__(self, __name__)
        # if not df_src:
        #     raise MandatoryParamError("Dataframe Empty")
        if not selected_blog_url:
            raise MandatoryParamError("Blog URL")

        self.df_src = df_src
        self.selected_blog_url = selected_blog_url

        self.logger.info("\nFetch Blogpost Interested Users")

    def process(self) -> DataFrame:
        selected_df = self.df_src.loc[self.df_src['PARENT_URL'] == self.selected_blog_url]

        users_of_interest = selected_df.filter(['AUTHOR_EMAIL', 'COMPOUND_SCORE'], axis=1)

        self.logger.debug("\n".join(["\nusers of interest: {}".format(len(users_of_interest))]))

        return selected_df
