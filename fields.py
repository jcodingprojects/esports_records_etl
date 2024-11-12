from enum import StrEnum
from pandas import Timestamp, Int64Dtype, Float64Dtype


class FieldEnum(StrEnum):
    """
    Hepler wrapper of StrEnum to reduce boilerplate 
    when querying and loading from database
    """
    @classmethod
    def get_suffixed(cls, suffix):
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


class DTypeEnum(StrEnum):
    @classmethod
    def get_fields(cls):
        pass

    @classmethod
    def split_dtypes(cls):
        """
        Split Dtypes into those which are coercible through pandas and 
        ones which will be treated separately.
        """
        fields = cls.get_fields()
        normal_dtypes, special_dtypes = {}, {}
        for name, value in cls.__members__.items():
            if name in fields:
                if value in ['String', 'Integer', 'Text', 'Datetime', 'Float']:
                    normal_dtypes[name] = dtype_wiki2pandas[value]
                else:
                    special_dtypes[name] = value
        return normal_dtypes, special_dtypes


class SPFields(FieldEnum):
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


class SPDTypes(DTypeEnum):
    @classmethod
    def get_fields(cls):
        return SPFields

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


class SGFields(FieldEnum):
    tournament = 'Tournament'
    team1 = 'Team1'
    team2 = 'Team2'
    win_team = 'WinTeam'
    loss_team = 'LossTeam'
    datetime_utc = 'DateTime_UTC'
    team_1_score = 'Team1Score'
    team_2_score = 'Team2Score'
    winner = 'Winner'
    game_length = 'Gamelength_Number'
    team_1_bans = 'Team1Bans'
    team_2_bans = 'Team2Bans'
    team_1_picks = 'Team1Picks'
    team_2_picks = 'Team2Picks'
    team_1_players = 'Team1Players'
    team_2_players = 'Team2Players'
    team_1_dragons = 'Team1Dragons'
    team_2_dragons = 'Team2Dragons'
    team_1_barons = 'Team1Barons'
    team_2_barons = 'Team2Barons'
    team_1_towers = 'Team1Towers'
    team_2_towers = 'Team2Towers'
    team_1_gold = 'Team1Gold'
    team_2_gold = 'Team2Gold'
    team_1_kills = 'Team1Kills'
    team_2_kills = 'Team2Kills'
    match_history = 'MatchHistory'
    game_name = 'Gamename'
    unique_line = 'UniqueLine'
    game_id = 'GameId'
    match_id = 'MatchId'
    riot_game_id = 'RiotGameId'


class SGDTypes(DTypeEnum):
    @classmethod
    def get_fields(cls):
        return SGFields

    OverviewPage = 'String'
    Tournament = 'String'
    Team1 = 'String'
    Team2 = 'String'
    WinTeam = 'String'
    LossTeam = 'String'
    DateTime_UTC = 'Datetime'
    DST = 'String'
    Team1Score = 'Integer'
    Team2Score = 'Integer'
    Winner = 'Integer'
    Gamelength = 'String'
    Gamelength_Number = 'Float'
    Team1Bans = 'List of String, delimiter: ,'
    Team2Bans = 'List of String, delimiter: ,'
    Team1Picks = 'List of String, delimiter: ,'
    Team2Picks = 'List of String, delimiter: ,'
    Team1Players = 'List of String, delimiter: ,'
    Team2Players = 'List of String, delimiter: ,'
    Team1Dragons = 'Integer'
    Team2Dragons = 'Integer'
    Team1Barons = 'Integer'
    Team2Barons = 'Integer'
    Team1Towers = 'Integer'
    Team2Towers = 'Integer'
    Team1Gold = 'Float'
    Team2Gold = 'Float'
    Team1Kills = 'Integer'
    Team2Kills = 'Integer'
    Team1RiftHeralds = 'Integer'
    Team2RiftHeralds = 'Integer'
    Team1VoidGrubs = 'Integer'
    Team2VoidGrubs = 'Integer'
    Team1Inhibitors = 'Integer'
    Team2Inhibitors = 'Integer'
    Patch = 'String'
    PatchSort = 'String'
    MatchHistory = 'String'
    VOD = 'Wikitext'
    N_Page = 'Integer'
    N_MatchInTab = 'Integer'
    N_MatchInPage = 'Integer'
    N_GameInMatch = 'Integer'
    Gamename = 'String'
    UniqueLine = 'String'
    GameId = 'String'
    MatchId = 'String'
    RiotPlatformGameId = 'String'
    RiotPlatformId = 'String'
    RiotGameId = 'String'
    RiotHash = 'String'
    RiotVersion = 'Integer'


dtype_wiki2psql = {
    'String': 'VARCHAR (256)',
    'Integer': 'INT',
    'Float': 'FLOAT',
    'Datetime': 'TIMESTAMP',
    'Text': 'VARCHAR (256)',
    'List of String, delimiter: ;': 'TEXT[]' ,
    'List of String, delimiter: ,': 'TEXT[]',
    'List of String delimiter: ;': 'TEXT[]'
}

dtype_wiki2pandas = {
    'String': 'string',
    'Text': 'string',
    'Integer': Int64Dtype(),
    'Float': Float64Dtype(),
    'Datetime': 'datetime64[s]'
}