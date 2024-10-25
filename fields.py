from enum import StrEnum



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
    gold = 'Gold'
    cs = 'CS'
    champ_damage = 'DamageToChampions'
    team_kills = 'TeamKills'
    team_gold = 'TeamGold'
    team = 'Team'
    team_vs = 'Team'
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