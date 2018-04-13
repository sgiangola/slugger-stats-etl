import data_utils


def parse_players(game):
    url = data_utils.get_game_url(game, 'players.xml')
    xml = data_utils.get_root_xml(url)
    if not xml:
        return []
    all_players = []
    for teams in xml.findall('team'):
        for player in teams.findall('player'):
            all_players.append({
                'player_id': player.attrib['id'],
                'first_name': player.attrib['first'],
                'last_name': player.attrib['last'],
                'team_id': int(player.attrib['team_id']),
                'display_name':
                player.attrib['first'] + ' ' + player.attrib['last']
            })
    return all_players


def player_insert_and_update(data):
    trunc_sql = 'TRUNCATE TABLE ' + data_utils.STG_PLAYER + ';'
    ins_sql = \
        '''
            INSERT INTO player \
            SELECT * FROM stg_player \
            ON CONFLICT (player_id) DO UPDATE SET \
            first_name = EXCLUDED.first_name, \
            last_name = EXCLUDED.last_name, \
            team_id = EXCLUDED.team_id, \
            display_name= EXCLUDED.display_name;
        '''
    data_utils.execute_sql(trunc_sql)
    data_utils.insert_to_db(data, data_utils.STG_PLAYER)
    data_utils.execute_sql(ins_sql)
