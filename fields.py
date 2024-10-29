from enum import StrEnum
from pandas import Timestamp


class FieldHolder(StrEnum):
    """
    Hepler wrapper of StrEnum to reduce boilerplate 
    when querying and loading from database
    """
    @classmethod
    def _get_suffixed(cls, suffix):
        """
        Return string of values delimited by , and joined by . 
        in format of psql query
        """
        return ', '.join([suffix + '.' + s.value for s in cls])

    @classmethod
    def invert(cls):
        """Return dictionary of inverted mapping"""
        return {value: name.name for value, name in cls._value2member_map_.items()}

    @classmethod
    def list_values(cls):
        """Return list of values"""
        return [s.value for s in cls]

    @classmethod
    def list_names(cls):
        """Return list of names"""
        return [s.name for s in cls]


class SPFields(FieldHolder):
    @classmethod
    def get_suffixed(cls):
        return cls._get_suffixed('SP')
    
    name = 'Name'
    champion = 'Champion'
    kills = 'Kills'
    deaths = 'Deaths'
    assists = 'Assists'
    summoner_spells = 'SummonerSpells'
    gold = 'Gold'
    cs = 'CS'
    champ_damage = 'DamageToChampions'
    vision_score = 'VisionScore'
    items = 'Items'
    keystone_mastery = 'KeystoneMastery'
    keystone_rune = 'KeystoneRune'
    primary_tree = 'PrimaryTree'
    secondary_tree = 'SecondaryTree'
    runes = 'Runes'
    team_kills = 'TeamKills'
    team_gold = 'TeamGold'
    team = 'Team'
    team_vs = 'TeamVs'
    game_time = 'Time'
    win = 'PlayerWin'
    datetime_utc = 'DateTime_UTC'
    tournament = 'Tournament'
    role = 'Role'
    role_num = 'Role_Number'
    in_game_role = 'IngameRole'
    side = 'Side'
    unique_line = 'UniqueLine'
    unique_line_vs = 'UniqueLineVs'
    unique_role = 'UniqueRole'
    unique_role_vs = 'UniqueRoleVs'
    game_id = 'GameId'
    match_id = 'MatchId'
    game_team_id = 'GameTeamId'
    game_role_id = 'GameRoleId'
    game_role_id_vs = 'GameRoleIdVs'



class SGFields(FieldHolder):
    @classmethod
    def get_suffixed(cls):
        return cls._get_suffixed('SG')

    tournament = 'Tournament'
    team1 = 'Team1'
    team2 = 'Team2'
    win_team = 'WinTeam'
    loss_team = 'LossTeam'
    datetime_utc = 'DateTime_UTC'
    team_1_score = 'Team1Score'
    team_2_score = 'Team2Score'
    game_length = 'Gamelength_Number'
    team_1_picks = 'Team1Picks'
    team_2_picks = 'Team2Picks'
    team_1_gold = 'Team1Gold'
    team_2_gold = 'Team2Gold'
    team_1_kills = 'Team1Kills'
    team_2_kills = 'Team2Kills'
    game_id = 'GameId'
    match_id = 'MatchId'


class DTypeEnum(StrEnum):
    @classmethod
    def dtypes_map(cls):
        return {
            name: dtype_wiki2pandas[value.value]
            for name, value in cls.__members__.items()
            if value in ['String', 'Integer', 'Text', 'Datetime']
        }

class SPDTypes(DTypeEnum):
    Name = 'String'
    Champion = 'String'
    Kills = 'Integer'
    Deaths = 'Integer'
    Assists = 'Integer'
    SummonerSpells = 'List of String, delimiter: ,'
    Gold = 'Integer'
    CS = 'Integer'
    DamageToChampions = 'Integer'
    VisionScore = 'Integer'
    Items = 'List of String delimiter: ;'
    KeystoneMastery = 'String'
    KeystoneRune = 'String'
    PrimaryTree = 'String'
    SecondaryTree = 'String'
    Runes = 'Text'
    TeamKills = 'Integer'
    TeamGold = 'Integer'
    Team = 'String'
    TeamVs = 'String'
    Time = 'Datetime'
    PlayerWin = 'String'
    DateTime_UTC = 'Datetime'
    Tournament = 'String'
    Role = 'String'
    Role_Number = 'Integer'
    IngameRole = 'String'
    Side = 'Integer'
    UniqueLine = 'String'
    UniqueLineVs = 'String'
    UniqueRole = 'String'
    UniqueRoleVs = 'String'
    GameId = 'String'
    MatchId = 'String'
    GameTeamId = 'String'
    GameRoleId = 'String'
    GameRoleIdVs = 'String'

dtype_wiki2psql = {
    'String': 'VARCHAR (256)',
    'Integer': 'INT',
    'Datetime': 'TIMESTAMP',
    'Text': 'VARCHAR (256)',
    'List of String, delimiter: ;': 'TEXT[]' ,
    'List of String, delimiter: ,': 'TEXT[]',
    'List of String delimiter: ;': 'TEXT[]'
}

dtype_wiki2pandas = {
    'String': str,
    'Text': str,
    'Integer': int,
    'Datetime': Timestamp

}

