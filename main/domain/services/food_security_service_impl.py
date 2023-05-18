import os

from pandas import DataFrame

from main.domain.repositories.email_repository import EmailRepository
from main.domain.services.food_security_service import FoodSecurityService


class FoodSecurityServiceImpl(FoodSecurityService):

    def __init__(self,
                 email_repository: EmailRepository,
                 region_country_df: DataFrame,
                 region_population_df: DataFrame,
                 email_country_df: DataFrame):
        self.region_country_df = region_country_df
        self.region_population_df = region_population_df
        self.email_country_df = email_country_df
        self.email_repository = email_repository

    def get_alerts(self, df_now, df_30_years_ago):
        food_security_df = self.get_food_security(df_now, df_30_years_ago)
        # self.craft_email(food_security_df)
        # self.craft_admin_email(food_security_df)
        return food_security_df

    def get_food_security(self, df_now, df_30_years_ago):
        security = df_now.merge(df_30_years_ago)
        security = security.merge(self.region_country_df, how='left', on='region_id')
        security = security.merge(self.region_population_df, how='left')
        # sum regions values of populations in the same country
        concatenated_regions = security.groupby('country_id')['region_id'].apply(lambda x: self.get_region_ids(x)).reset_index()
        security_by_country = security.drop('region_id', axis=1).groupby('country_id').sum().reset_index()
        security_by_country = security_by_country.merge(concatenated_regions)
        #
        security_by_country['percentage_difference'] = (security_by_country["food_insecure_people"] - security_by_country["food_insecure_people_30_days_ago"]) / \
                                                       security_by_country['population'] * 100
        security_by_country['>=5%'] = security_by_country['percentage_difference'] >= 5
        security_by_country = security_by_country[security_by_country['>=5%'] == True]

        security_with_email = security_by_country.merge(self.email_country_df)
        return security_with_email

    def get_region_ids(self, x):
        regions = x.tolist()
        regions_str = [str(region) for region in regions]
        return '-'.join(regions_str)

    def craft_email(self, security_df):
        for index, row in security_df.iterrows():
            email = row['email']
            country = row['country_id']
            insecure_people = row['food_insecure_people']
            insecure_people_30 = row['food_insecure_people_30_days_ago']
            population = row['population']
            increase = row['percentage_difference']

            # if we want to craft more personal messages like this use a QUEUE else if bulk, we can use an email list
            subject = "Alert: Food Security"
            body = f"Food security has decreased significantly in your country: {country}" \
                   f"\nFood insecure people {insecure_people} over the " \
                   f"\nTotal population {population} " \
                   f"\nHas increased by {increase} % compared to 30 days ago " \
                   f"\nwhich was {insecure_people_30}"
            self.email_repository.send_email(email, subject, body)

    def craft_admin_email(self, security_df):
        email = os.getenv("admin_email")
        food_security_json = security_df.to_json(orient="records")

        subject = "Alert: Food Security"
        body = f"Food security has decreased significantly in the following countries according to the below statistics:" \
               f"\n{food_security_json}"
        self.email_repository.send_email(email, subject, body)
