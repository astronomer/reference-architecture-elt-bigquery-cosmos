import pandas as pd
import numpy as np
from faker import Faker
import random
import uuid

fake = Faker()


def generate_uuids(n):
    return [str(uuid.uuid4()) for _ in range(n)]


def generate_users_data(num_users=100, date=None, filename="users.csv"):
    data = {
        "user_id": generate_uuids(num_users),
        "user_name": [fake.name() for _ in range(num_users)],
        "date_of_birth": [
            fake.date_of_birth(minimum_age=18, maximum_age=70) for _ in range(num_users)
        ],
        "sign_up_date": [
            fake.date_between(start_date="-3y", end_date="today")
            for _ in range(num_users)
        ],
        "updated_at": date,
    }
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return df


def generate_cheeses_data(filename="cheeses.csv", date=None):
    uuids = [str(uuid.uuid4()) for _ in range(30)]

    # List of 30 cheeses with at least 15 Swiss cheeses
    swiss_cheeses = [
        "Emmental",
        "Gruyère",
        "Appenzeller",
        "Tête de Moine",
        "Sbrinz",
        "Raclette",
        "Tilsit",
        "Vacherin Fribourgeois",
        "Berner Alpkäse",
        "L'Etivaz",
        "Schabziger",
        "Formaggio d'Alpe Ticinese",
        "Bleuchâtel",
        "Vacherin Mont d'Or",
        "Tomme Vaudoise",
    ]

    other_cheeses = [
        "Cheddar",
        "Brie",
        "Camembert",
        "Gouda",
        "Parmesan",
        "Mozzarella",
        "Blue Cheese",
        "Feta",
        "Goat Cheese",
        "Provolone",
        "Manchego",
        "Roquefort",
        "Pecorino",
        "Havarti",
        "Asiago",
    ]

    cheese_names = swiss_cheeses + other_cheeses
    cheese_types = ["Swiss"] * len(swiss_cheeses) + ["Other"] * len(other_cheeses)

    prices = np.round(np.random.uniform(5.0, 25.0, size=len(cheese_names)), 2)

    data = {
        "cheese_id": uuids,
        "cheese_name": cheese_names,
        "cheese_type": cheese_types,
        "price": prices,
        "updated_at": date,
    }
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return df


def generate_utm_data(num_utms=100, date=None, filename="utms.csv"):
    utm_sources = ["Google", "Facebook", "Twitter", "LinkedIn", "Instagram"]
    utm_mediums = ["CPC", "email", "social", "referral", "banner"]
    utm_campaigns = [
        "Cheese_Festival",
        "Winter_Discount",
        "Black_Friday",
        "Holiday_Special",
        "Spring_Promo",
    ]
    utm_terms = ["cheese", "gourmet", "artisan", "dairy", "delicacy"]
    utm_contents = ["image1", "image2", "text_link", "button_click", None]

    data = {
        "utm_id": generate_uuids(num_utms),
        "utm_source": random.choices(utm_sources, k=num_utms),
        "utm_medium": random.choices(utm_mediums, k=num_utms),
        "utm_campaign": random.choices(utm_campaigns, k=num_utms),
        "utm_term": random.choices(utm_terms, k=num_utms),
        "utm_content": random.choices(utm_contents, k=num_utms),
        "updated_at": date,
    }
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return df


def generate_sales_data(
    num_sales=1000,
    users_df=None,
    cheeses_df=None,
    utm_df=None,
    filename="sales.csv",
    date=None,
):
    if num_sales == 1:
        data = {
            "sale_id": generate_uuids(num_sales),
            "user_id": users_df["user_id"].values[0],
            "cheese_id": cheeses_df["cheese_id"].values[0],
            "utm_id": utm_df["utm_id"].values[0],
            "quantity": np.random.randint(1, 10),
            "sale_date": date.strftime("%Y-%m-%d %H:%M:%S") if date else None,
        }
    elif num_sales > 1:
        data = {
            "sale_id": generate_uuids(num_sales),
            "user_id": np.random.choice(users_df["user_id"], size=num_sales),
            "cheese_id": np.random.choice(cheeses_df["cheese_id"], size=num_sales),
            "utm_id": np.random.choice(utm_df["utm_id"], size=num_sales),
            "quantity": np.random.randint(1, 10, size=num_sales),
            "sale_date": [
                fake.date_time_between(start_date="-1y", end_date="now").strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                for _ in range(num_sales)
            ],
        }
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return df


def get_new_sales_from_internal_api(num_sales, date):
    num_users = int(0.75 * num_sales)
    if num_users < 1:
        num_users = 1
    users_df = generate_users_data(num_users=num_users, date=date)
    cheeses_df = generate_cheeses_data(date=date)
    utm_df = generate_utm_data(num_utms=num_users, date=date)
    sales_df = generate_sales_data(
        num_sales=num_sales,
        users_df=users_df,
        cheeses_df=cheeses_df,
        utm_df=utm_df,
        date=date,
    )
    return sales_df, users_df, cheeses_df, utm_df
