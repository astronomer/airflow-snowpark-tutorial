"""Create a dataset of skiers and their attributes to predict their 
afternoon beverage type."""

import pandas as pd
import numpy as np


def generate_dataset(num_entries=2000, special_cluster_sizes=[100, 600, 600]):
    resorts = [
        "Zermatt",
        "St. Moritz",
        "Jungfrau",
        "Davos",
        "Verbier",
        "Arosa",
        "Laax",
        "Engelberg",
        "Lenzerheide",
        "Crans-Montana",
    ]
    colors = [
        "red",
        "blue",
        "green",
        "yellow",
        "black",
        "white",
        "grey",
        "purple",
        "orange",
        "pink",
    ]
    beverages_no_choco = [
        "tea",
        "coffee",
        "snow_mocha",
        "orange juice",
        "wine",
        "water",
    ]

    skier_ids = np.array(range(1, num_entries + 1))
    skier_resorts = np.random.choice(resorts, num_entries)
    skier_jackets = np.random.choice(colors, num_entries)
    skier_skis = np.random.choice(colors, num_entries)
    hours_skied = np.random.randint(1, 10, num_entries)
    snow_quality = np.random.randint(1, 11, num_entries)
    had_lunch = np.random.choice([True, False], num_entries)
    cm_snow_night_before = np.random.randint(0, 50, num_entries)

    beverage_choices = []
    for i in range(num_entries):
        if hours_skied[i] > 5 and skier_skis[i] == "blue":
            choice = "hot_chocolate"
        elif hours_skied[i] < 2 and skier_resorts[i] in ["Zermatt", "Verbier"]:
            choice = "tea"
        elif 3 <= hours_skied[i] <= 5 and skier_resorts[i] in ["St. Moritz", "Davos"]:
            choice = "snow_mocha"
        else:
            choice = np.random.choice(beverages_no_choco)

        beverage_choices.append(choice)

    for i, size in enumerate(special_cluster_sizes):
        for _ in range(size):
            skier_ids = np.append(skier_ids, max(skier_ids) + 1)

            if i == 0:  # Hot Chocolate Cluster
                skier_resorts = np.append(skier_resorts, np.random.choice(resorts))
                skier_skis = np.append(
                    skier_skis, np.random.choice(["blue", "green"], p=[0.6, 0.4])
                )
                skier_jackets = np.append(skier_jackets, np.random.choice(colors))
                hours_skied = np.append(hours_skied, np.random.randint(4, 9))
                had_lunch = np.append(had_lunch, False)
                snow_quality = np.append(snow_quality, np.random.randint(1, 11))
                cm_snow_night_before = np.append(
                    cm_snow_night_before, np.random.randint(0, 50)
                )
                choice = "hot_chocolate"

            elif i == 1:  # Tea Lovers Cluster
                skier_resorts = np.append(
                    skier_resorts, np.random.choice(["Zermatt", "Verbier"])
                )
                skier_skis = np.append(skier_skis, np.random.choice(colors))
                skier_jackets = np.append(skier_jackets, np.random.choice(colors))
                hours_skied = np.append(hours_skied, np.random.randint(1, 3))
                snow_quality = np.append(snow_quality, np.random.randint(1, 11))
                had_lunch = np.append(had_lunch, np.random.choice([True, False]))
                cm_snow_night_before = np.append(
                    cm_snow_night_before, np.random.randint(0, 50)
                )
                choice = "tea"

            else:  # Snow Mocha Enthusiasts
                skier_resorts = np.append(
                    skier_resorts, np.random.choice(["St. Moritz", "Davos"])
                )
                skier_skis = np.append(skier_skis, np.random.choice(colors))
                skier_jackets = np.append(skier_jackets, np.random.choice(colors))
                hours_skied = np.append(hours_skied, np.random.randint(3, 5))
                snow_quality = np.append(snow_quality, np.random.randint(1, 11))
                had_lunch = np.append(had_lunch, np.random.choice([True, False]))
                cm_snow_night_before = np.append(
                    cm_snow_night_before, np.random.randint(0, 50)
                )
                choice = "snow_mocha"

            beverage_choices = np.append(beverage_choices, choice)

    df = pd.DataFrame(
        {
            "skier_id": skier_ids,
            "resort": skier_resorts,
            "jacket_color": skier_jackets,
            "ski_color": skier_skis,
            "hours_skied": hours_skied,
            "snow_quality": snow_quality,
            "had_lunch": had_lunch,
            "cm_of_new_snow": cm_snow_night_before,
            "afternoon_beverage": beverage_choices,
        }
    )

    return df


df = generate_dataset()
df.to_csv("ski_dataset.csv", index=False)
print(df.head())
