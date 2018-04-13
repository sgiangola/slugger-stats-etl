import data_utils


def parse_stats(game):
    game_url = data_utils.get_game_url(game, 'rawboxscore.xml')
    boxscore = data_utils.get_root_xml(game_url)
    if not boxscore:
        return []
    batter_stats = []
    for team in boxscore.findall('team'):
        for batting in team.findall('batting'):
            batters = batting.findall('batter')
            for batter in batters:
                batter_stats.append({
                    'player_id': int(batter.attrib['id']),
                    'team_id': team.attrib['id'],
                    'pos': batter.attrib['pos'],
                    'game_id': game[1],
                    'game_date': game[0],
                    'hr': int(batter.attrib['hr']),
                    'ab': int(batter.attrib['ab']),
                    'r': int(batter.attrib['r']),
                    'h': int(batter.attrib['h']),
                    'rbi': int(batter.attrib['rbi']),
                    'bb': int(batter.attrib['bb']),
                    'so': int(batter.attrib['so']),
                    'bis_avg': float(batter.attrib['bis_avg']),
                    'bam_avg': float(batter.attrib['bis_avg']),
                    'bis_obp': float(batter.attrib['bis_obp']),
                    'bam_obp': float(batter.attrib['bam_obp']),
                    'bis_slg': float(batter.attrib['bis_slg']),
                    'bam_slg': float(batter.attrib['bam_obp']),
                })
    return batter_stats


def upsert(data):
    trunc_sql = 'TRUNCATE TABLE ' + data_utils.STG_PLAYER_BATTING + ';'
    data_utils.execute_sql(trunc_sql)
    data_utils.insert_to_db(data, data_utils.STG_PLAYER_BATTING)
    ins_sql = \
        '''
            INSERT INTO player_batting \
            SELECT * FROM stg_player_batting \
            ON CONFLICT (player_id, game_id) DO UPDATE SET \
                hr=EXCLUDED.hr,
                ab=EXCLUDED.ab,
                r=EXCLUDED.r,
                h=EXCLUDED.h,
                rbi=EXCLUDED.rbi,
                bb=EXCLUDED.bb,
                so=EXCLUDED.so,
                bis_avg=EXCLUDED.bis_avg,
                bam_avg=EXCLUDED.bam_avg,
                bis_obp=EXCLUDED.bis_obp,
                bam_obp=EXCLUDED.bam_obp,
                bis_slg=EXCLUDED.bis_slg,
                bam_slg=EXCLUDED.bam_slg
        '''
    data_utils.execute_sql(ins_sql)
