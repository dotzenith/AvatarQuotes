"""
Tests to make sure the dataset is valid
"""

import polars as pl
from pathlib import Path
import subprocess

def get_git_repo() -> str:
    """
    Returns the top-level git repo's path
    """

    return (
        subprocess.Popen(
            ["git", "rev-parse", "--show-toplevel"], stdout=subprocess.PIPE
        )
        .communicate()[0]
        .rstrip()
        .decode("utf-8")
    )


def make_dataframe() -> pl.DataFrame:
    """
    Makes a DataFrame from all of the CSV files from all seasons
    """

    git_path = get_git_repo()
    sub_dirs = [
        f"{git_path}/books/01_Water",
        f"{git_path}/books/02_Earth",
        f"{git_path}/books/03_Fire",
    ]

    dfs = []
    for sub_dir in sub_dirs:
        files = Path(sub_dir).glob("*.csv")
        for file in files:
            dfs.append(pl.read_csv(file, separator="|"))

    return pl.concat(dfs)


class TestDataset:
    """
    A class for all the tests to live in
    """

    dataframe = make_dataframe()

    def test_no_elements_are_nan(self):
        """
        A test to make sure no elements in the datset are NaN
        """

        assert any(col.has_nulls() for col in self.dataframe) is False

    def test_character_names(self):
        """
        A test to make sure the characters listed below are the only characters in the dataset
        """

        expected_characters = {
            "Katara",
            "Sokka",
            "Zuko",
            "Merchant woman",
            "Aang",
            "Fisherman's wife",
            "Fisherman",
            "Jee",
            "Iroh",
            "Tashi",
            "Pasang",
            "Gyatso",
            "Ozai",
            "Yue",
            "Pakku",
            "Oh",
            "Pirate captain",
            "Cabbage merchant",
            "Gan Jin tribesman",
            "Zhang leader",
            "Gan Jin leader",
            "Canyon guide",
            "Hakoda",
            "Bato",
            "Chey",
            "Jeong Jeong",
            "Bumi",
            "Jet",
            "Kanna",
            "Southern Water Tribe girl",
            "Mechanist",
            "Haru",
            "Guard",
            "Tyro",
            "Captain",
            "Warden",
            "Suki",
            "Meng",
            "Wu",
            "Zhao",
            "Kay-fon",
            "Senlin Village leader",
            "Earthbender captain",
            "Great Fire Sage",
            "Shyu",
            "Roku",
            "Arnook",
            "Hahn",
            "Koh",
            "Chong",
            "Lily",
            "Song",
            "Tho",
            "Huu",
            "Due",
            "Tong",
            "Kyoshi",
            "Zei",
            "Toph",
            "Wan Shi Tong",
            "Ticket lady",
            "Ying",
            "Smellerbee",
            "jet",
            "Azula",
            "Fong",
            "Kuei",
            "Guru Pathik",
            "Ty Lee",
            "Mai",
            "Kenji",
            "Macmu-Ling",
            "Joo Dee",
            "Long Feng",
            "General Sung",
            "Pong",
            "Yu",
            "The Boulder",
            "Yung",
            "Iio",
            "Lee",
            "Gansu",
            "Ursa",
            "Kwan",
            "Piandao",
            "Kuruk",
            "Yangchen",
            "Lion Turtle",
            "Hama",
            "Momo",
            "Appa",
        }
        df_characters = set(self.dataframe["character"].explode().unique())

        assert expected_characters == df_characters

    def test_nations(self):
        """
        A test to assure that ["Water", "Earth", "Fire", "Air", "None"] are the only nations
        """

        expected_nations = {"Water", "Earth", "Fire", "Air", "None"}
        df_nations = set(self.dataframe["nation"].explode().unique())

        assert expected_nations == df_nations

    def test_bending_styles(self):
        """
        A test to assure that ["Water", "Earth", "Fire", "Air", "None", "All"] are the only bending types
        """

        expected_bending_types = {"Water", "Earth", "Fire", "Air", "None", "All"}
        df_bending_types = set(self.dataframe["bending"].explode().unique())

        assert expected_bending_types == df_bending_types

    def test_episodes(self):
        """
        A test to make sure the episodes listed below are the only episodes in the dataset
        """

        expected_episodes = {
            "The Storm",
            "The Waterbending Master",
            "The Waterbending Scroll",
            "The Great Divide",
            "Bato of the Water Tribe",
            "The Deserter",
            "The King of Omashu",
            "Jet",
            "The Avatar Returns",
            "The Northern Air Temple",
            "The Boy in the Iceberg",
            "Imprisoned",
            "The Warriors of Kyoshi",
            "The Fortuneteller",
            "The Southern Air Temple",
            "Winter Solstice Part 1: The Spirit World",
            "Winter Solstice Part 2: Avatar Roku",
            "The Siege of the North, Part 1",
            "The Siege of the North, Part 2",
            "The Blue Spirit",
            "The Cave of Two Lovers",
            "The Swamp",
            "Avatar Day",
            "The Library",
            "The Serpent's Pass",
            "The Avatar State",
            "The Guru",
            "The Tales of Ba Sing Se",
            "Lake Laogai",
            "Bitter Work",
            "The Earth King",
            "The Drill",
            "City of Walls and Secrets",
            "The Blind Bandit",
            "The Chase",
            "The Crossroads of Destiny",
            "Return to Omashu",
            "Appa's Lost Days",
            "The Desert",
            "Zuko Alone",
            "The Firebending Masters",
            "The Awakening",
            "The Headband",
            "The Western Air Temple",
            "The Runaway",
            "The Day of Black Sun, Part 1: The Invasion",
            "The Beach",
            "The Boiling Rock, Part 2",
            "Sozin's Comet, Part 2: The Old Masters",
            "The Painted Lady",
            "Sozin's Comet, Part 4: Avatar Aang",
            "Sozin's Comet, Part 1: The Phoenix King",
            "The Puppetmaster",
            "The Boiling Rock, Part 1",
            "Nightmares and Daydreams",
            "The Avatar and The Fire Lord",
            "The Day of Black Sun, Part 2: The Eclipse",
            "Sozin's Comet, Part 3: Into the Inferno",
            "Sokka's Master",
            "The Southern Raiders",
            "The Ember Island Players",
        }
        df_episodes = set(self.dataframe["episode"].explode().unique())

        assert expected_episodes == df_episodes

    def test_books(self):
        """
        A test to assure that ["Water", "Earth", "Fire"] are the only books in the dataset
        """

        expected_books = {"Water", "Earth", "Fire"}
        df_books = set(self.dataframe["book"].explode().unique())

        assert expected_books == df_books
